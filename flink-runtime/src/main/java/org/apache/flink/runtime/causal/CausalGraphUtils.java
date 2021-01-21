/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 *
 *
 */

package org.apache.flink.runtime.causal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CausalGraphUtils {

	public static VertexID computeVertexId(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID,
										   int subtaskIndex) {
		short idCounter = 0;
		for (JobVertex jobVertex : sortedJobVertexes) {
			if (jobVertex.getID().equals(jobVertexID)) {
				idCounter += subtaskIndex;
				break;
			}
			idCounter += jobVertex.getParallelism();
		}
		return new VertexID(idCounter);
	}

	public static JobVertex fromSortedList(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID) {
		Map<JobVertexID, JobVertex> map = sortedJobVertexes.stream().collect(Collectors.toMap(JobVertex::getID,
			jobVertex -> jobVertex));
		return map.get(jobVertexID);
	}

	private static List<VertexID> toVertexIdList(List<JobVertex> sortedList, List<JobVertex> jobVertexesToCompute) {
		return jobVertexesToCompute.stream().flatMap(v -> IntStream.range(0, v.getParallelism()).boxed().map(i -> computeVertexId(sortedList, v.getID(), i))).collect(Collectors.toList());
	}


	public static List<JobVertex> computeUpstreamJobVertexes(List<JobVertex> sortedJobVertexes,
															 JobVertexID jobVertexID) {
		JobVertex target = fromSortedList(sortedJobVertexes, jobVertexID);

		List<JobVertex> upstreamVertexes = new LinkedList<>();
		Deque<JobVertex> unexplored =
			target.getInputs().stream().map(je -> je.getSource().getProducer()).distinct().collect(Collectors.toCollection(ArrayDeque::new));

		while (!unexplored.isEmpty()) {
			JobVertex toExplore = unexplored.pop();
			upstreamVertexes.add(toExplore);

			toExplore.getInputs().forEach(jobEdge -> unexplored.add(jobEdge.getSource().getProducer()));
		}

		return upstreamVertexes.stream().distinct().collect(Collectors.toList());
	}


	private static void breadthFirstSearch(Map<VertexID, Integer> toUpdate, List<JobVertex> sortedJobVertexes, JobVertex start, BiFunction<JobVertex, Integer, Stream<Tuple2<Integer, JobVertex>>> explorer, BiFunction<Integer, Integer, Integer> merger) {

		Deque<Tuple2<Integer, JobVertex>> unexplored = new ArrayDeque<>(50);
		unexplored.push(Tuple2.of(0, start));

		while (!unexplored.isEmpty()) {

			Tuple2<Integer, JobVertex> distAndToExplore = unexplored.pop();
			JobVertex toExplore = distAndToExplore.f1;

			int distance = 0;
			for (int i = 0; i < toExplore.getParallelism(); i++) {
				VertexID vertexID = computeVertexId(sortedJobVertexes, toExplore.getID(), i);
				distance = toUpdate.merge(vertexID, distAndToExplore.f0, merger);
			}

			unexplored.addAll(explorer.apply(toExplore, distance).collect(Collectors.toList()));
		}
	}

	public static Map<VertexID, Integer> computeDistances(List<JobVertex> sortedJobVertexes, JobVertexID jobVertexID) {
		JobVertex localJobVertex = fromSortedList(sortedJobVertexes, jobVertexID);
		HashMap<VertexID, Integer> distances = new HashMap<>();

		//Upstream
		BiFunction<JobVertex, Integer, Stream<Tuple2<Integer, JobVertex>>> f1 = (j, d) ->
			j.getInputs().stream().map(je -> je.getSource().getProducer()).distinct().map(je -> Tuple2.of(d - 1, je));
		breadthFirstSearch(distances, sortedJobVertexes, localJobVertex, f1, Math::max);


		//Downstream
		BiFunction<JobVertex, Integer, Stream<Tuple2<Integer, JobVertex>>> f2 = (j, d) ->
			j.getProducedDataSets().stream().flatMap(ds -> ds.getConsumers().stream().map(JobEdge::getTarget))
				.distinct().map(je -> Tuple2.of(d + 1, je));
		breadthFirstSearch(distances, sortedJobVertexes, localJobVertex, f2, Math::min);

		return distances;
	}
}
