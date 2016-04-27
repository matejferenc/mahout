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

package org.apache.mahout.cf.taste.example.kddcup.track2;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.example.kddcup.KDDCupDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

final class Track2RecommenderBuilder implements RecommenderBuilder {

	@Override
	public Recommender buildRecommender(DataModel dataModel) throws TasteException {
		return new Track2Recommender(dataModel, ((KDDCupDataModel) dataModel).getDataFileDirectory());
	}

	public String getName() {
		return "Track 2 Recommender Builder";
	}

	@Override
	public void freeReferences() {
	}

	@Override
	public String getShortName() {
		return null;
	}

}
