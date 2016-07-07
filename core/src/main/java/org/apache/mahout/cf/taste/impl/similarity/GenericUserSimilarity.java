/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.cf.taste.impl.similarity;

import java.util.Collection;
import java.util.Iterator;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.IntPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.recommender.TopItems;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.PreferenceInferrer;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

public final class GenericUserSimilarity implements UserSimilarity {

	private final FastByIDMap<FastByIDMap<Double>> similarityMaps = new FastByIDMap<FastByIDMap<Double>>();

	public GenericUserSimilarity(Iterable<UserUserSimilarity> similarities) {
		initSimilarityMaps(similarities.iterator());
	}

	public GenericUserSimilarity(Iterable<UserUserSimilarity> similarities, int maxToKeep) {
		Iterable<UserUserSimilarity> keptSimilarities = TopItems.getTopUserUserSimilarities(maxToKeep, similarities.iterator());
		initSimilarityMaps(keptSimilarities.iterator());
	}

	public GenericUserSimilarity(UserSimilarity otherSimilarity, DataModel dataModel) throws TasteException {
		Integer[] userIDs = intIteratorToList(dataModel.getUserIDs());
		initSimilarityMaps(new DataModelSimilaritiesIterator(otherSimilarity, userIDs));
	}

	public GenericUserSimilarity(UserSimilarity otherSimilarity, DataModel dataModel, int maxToKeep) throws TasteException {
		Integer[] userIDs = intIteratorToList(dataModel.getUserIDs());
		Iterator<UserUserSimilarity> it = new DataModelSimilaritiesIterator(otherSimilarity, userIDs);
		Iterable<UserUserSimilarity> keptSimilarities = TopItems.getTopUserUserSimilarities(maxToKeep, it);
		initSimilarityMaps(keptSimilarities.iterator());
	}

	static Integer[] intIteratorToList(IntPrimitiveIterator iterator) {
		Integer[] result = new Integer[5];
		int size = 0;
		while (iterator.hasNext()) {
			if (size == result.length) {
				Integer[] newResult = new Integer[result.length << 1];
				System.arraycopy(result, 0, newResult, 0, result.length);
				result = newResult;
			}
			result[size++] = iterator.next();
		}
		if (size != result.length) {
			Integer[] newResult = new Integer[size];
			System.arraycopy(result, 0, newResult, 0, size);
			result = newResult;
		}
		return result;
	}

	private void initSimilarityMaps(Iterator<UserUserSimilarity> similarities) {
		while (similarities.hasNext()) {
			UserUserSimilarity uuc = similarities.next();
			Integer similarityUser1 = uuc.getUserID1();
			Integer similarityUser2 = uuc.getUserID2();
			if (similarityUser1 != similarityUser2) {
				// Order them -- first key should be the "smaller" one
				Integer user1;
				Integer user2;
				if (similarityUser1 < similarityUser2) {
					user1 = similarityUser1;
					user2 = similarityUser2;
				} else {
					user1 = similarityUser2;
					user2 = similarityUser1;
				}
				FastByIDMap<Double> map = similarityMaps.get(user1);
				if (map == null) {
					map = new FastByIDMap<Double>();
					similarityMaps.put(user1, map);
				}
				map.put(user2, uuc.getValue());
			}
			// else similarity between user and itself already assumed to be 1.0
		}
	}

	@Override
	public double userSimilarity(Integer userID1, Integer userID2) {
		if (userID1.equals(userID2)) {
			return 1.0;
		}
		long first;
		long second;
		if (userID1 < userID2) {
			first = userID1;
			second = userID2;
		} else {
			first = userID2;
			second = userID1;
		}
		FastByIDMap<Double> nextMap = similarityMaps.get(first);
		if (nextMap == null) {
			return Double.NaN;
		}
		Double similarity = nextMap.get(second);
		return similarity == null ? Double.NaN : similarity;
	}

	@Override
	public void setPreferenceInferrer(PreferenceInferrer inferrer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void refresh(Collection<Refreshable> alreadyRefreshed) {
		// Do nothing
	}

	public static final class UserUserSimilarity implements Comparable<UserUserSimilarity> {

		private final Integer userID1;
		private final Integer userID2;
		private final double value;

		public UserUserSimilarity(Integer userID1, Integer userID2, double value) {
			Preconditions.checkArgument(value >= -1.0 && value <= 1.0, "Illegal value: " + value + ". Must be: -1.0 <= value <= 1.0");
			this.userID1 = userID1;
			this.userID2 = userID2;
			this.value = value;
		}

		public Integer getUserID1() {
			return userID1;
		}

		public Integer getUserID2() {
			return userID2;
		}

		public double getValue() {
			return value;
		}

		@Override
		public String toString() {
			return "UserUserSimilarity[" + userID1 + ',' + userID2 + ':' + value + ']';
		}

		/** Defines an ordering from highest similarity to lowest. */
		@Override
		public int compareTo(UserUserSimilarity other) {
			double otherValue = other.getValue();
			return value > otherValue ? -1 : value < otherValue ? 1 : 0;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof UserUserSimilarity)) {
				return false;
			}
			UserUserSimilarity otherSimilarity = (UserUserSimilarity) other;
			return otherSimilarity.getUserID1().equals(userID1) && otherSimilarity.getUserID2().equals(userID2) && otherSimilarity.getValue() == value;
		}

		@Override
		public int hashCode() {
			return (int) userID1 ^ (int) userID2 ^ RandomUtils.hashDouble(value);
		}

	}

	private static final class DataModelSimilaritiesIterator extends AbstractIterator<UserUserSimilarity> {

		private final UserSimilarity otherSimilarity;
		private final Integer[] itemIDs;
		private int i;
		private Integer itemID1;
		private int j;

		private DataModelSimilaritiesIterator(UserSimilarity otherSimilarity, Integer[] itemIDs) {
			this.otherSimilarity = otherSimilarity;
			this.itemIDs = itemIDs;
			i = 0;
			itemID1 = itemIDs[0];
			j = 1;
		}

		@Override
		protected UserUserSimilarity computeNext() {
			int size = itemIDs.length;
			while (i < size - 1) {
				Integer itemID2 = itemIDs[j];
				double similarity;
				try {
					similarity = otherSimilarity.userSimilarity(itemID1, itemID2);
				} catch (TasteException te) {
					// ugly:
					throw new IllegalStateException(te);
				}
				if (!Double.isNaN(similarity)) {
					return new UserUserSimilarity(itemID1, itemID2, similarity);
				}
				if (++j == size) {
					itemID1 = itemIDs[++i];
					j = i + 1;
				}
			}
			return endOfData();
		}

	}

	@Override
	public String getName() {
		return "Generic User Similarity";
	}

	@Override
	public String getShortName() {
		return null;
	}

}
