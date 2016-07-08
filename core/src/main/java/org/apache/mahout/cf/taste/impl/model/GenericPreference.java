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

package org.apache.mahout.cf.taste.impl.model;

import java.io.Serializable;

import org.apache.mahout.cf.taste.model.Preference;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A simple {@link Preference} encapsulating an item and preference value.
 * </p>
 */
public class GenericPreference implements Preference, Serializable {

	private final Integer userID;
	private final Integer itemID;
	private double value;

	public GenericPreference(Integer userID, Integer itemID, double value) {
		Preconditions.checkArgument(!Double.isNaN(value), "NaN value");
		this.userID = userID;
		this.itemID = itemID;
		this.value = value;
	}

	@Override
	public Integer getUserID() {
		return userID;
	}

	@Override
	public Integer getItemID() {
		return itemID;
	}

	@Override
	public double getValue() {
		return value;
	}

	@Override
	public void setValue(double value) {
		Preconditions.checkArgument(!Double.isNaN(value), "NaN value");
		this.value = value;
	}

	@Override
	public String toString() {
		return "GenericPreference[userID: " + userID + ", itemID:" + itemID + ", value:" + value + ']';
	}

}
