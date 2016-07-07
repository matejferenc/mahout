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

package org.apache.mahout.cf.taste.impl.model.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.IntPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.AbstractDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.common.iterator.FileLineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

public class MatrixDataModel extends AbstractDataModel {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(MatrixDataModel.class);

	private static final char[] DELIMIETERS = { ',', '\t' };

	private final File dataFile;
	private final char delimiter;
	private final Splitter delimiterPattern;
	private DataModel delegate;

	private Integer firstLineTokenCount;

	private float nullPreferenceValue;

	public MatrixDataModel(File dataFile) throws IOException {
		this(dataFile, null, 0);
	}

	/**
	 * @param delimiterRegex
	 *            If your data file don't use '\t' or ',' as delimiters, you can specify user own using regex pattern.
	 * @throws IOException
	 */
	public MatrixDataModel(File dataFile, String delimiterRegex, float nullPreferenceValue) throws IOException {

		this.nullPreferenceValue = nullPreferenceValue;
		this.dataFile = Preconditions.checkNotNull(dataFile.getAbsoluteFile());
		if (!dataFile.exists() || dataFile.isDirectory()) {
			throw new FileNotFoundException(dataFile.toString());
		}
		Preconditions.checkArgument(dataFile.length() > 0L, "dataFile is empty");

		log.info("Creating FileDataModel for file {}", dataFile);

		FileLineIterator iterator = new FileLineIterator(dataFile, false);
		String firstLine = iterator.peek();
		while (firstLine.isEmpty()) {
			iterator.next();
			firstLine = iterator.peek();
		}
		Closeables.close(iterator, true);

		if (delimiterRegex == null) {
			delimiter = determineDelimiter(firstLine);
			delimiterPattern = Splitter.on(delimiter);
		} else {
			delimiter = '\0';
			delimiterPattern = Splitter.onPattern(delimiterRegex);
			if (!delimiterPattern.split(firstLine).iterator().hasNext()) {
				throw new IllegalArgumentException("Did not find a delimiter(pattern) in first line");
			}
		}
		List<String> firstLineSplit = Lists.newArrayList();
		for (String token : delimiterPattern.split(firstLine)) {
			firstLineSplit.add(token);
		}

		reload();
	}

	public File getDataFile() {
		return dataFile;
	}

	protected void reload() {
		try {
			delegate = buildModel();
		} catch (IOException ioe) {
			log.warn("Exception while reloading", ioe);
		}
	}

	protected DataModel buildModel() throws IOException {
		FastByIDMap<Collection<Preference>> data = new FastByIDMap<Collection<Preference>>();
		FileLineIterator iterator = new FileLineIterator(dataFile, false);
		processFile(iterator, data);

		return new GenericDataModel(GenericDataModel.toDataMap(data, true));

	}

	public static char determineDelimiter(String line) {
		for (char possibleDelimieter : DELIMIETERS) {
			if (line.indexOf(possibleDelimieter) >= 0) {
				return possibleDelimieter;
			}
		}
		throw new IllegalArgumentException("Did not find a delimiter in first line");
	}

	protected void processFile(FileLineIterator dataOrUpdateFileIterator, FastByIDMap<?> data) {
		log.info("Reading file info...");
		int count = 0;
		while (dataOrUpdateFileIterator.hasNext()) {
			String line = dataOrUpdateFileIterator.next();
			if (!line.isEmpty()) {
				processLine(line, data, count + 1);
				if (++count % 10000 == 0) {
					log.info("Processed {} lines", count);
				}
			}
		}
		log.info("Read lines: {}", count);
	}

	/**
	 * <p>
	 * Reads one line from the input file and adds the data to a {@link FastByIDMap} data structure which maps user IDs to preferences. This assumes that each line of the input file corresponds to one preference. After
	 * reading a line and determining which user and item the preference pertains to, the method should look to see if the data contains a mapping for the user ID already, and if not, add an empty data structure of
	 * preferences as appropriate to the data.
	 * </p>
	 *
	 * @param line
	 *            line from input data file
	 * @param data
	 *            all data read so far, as a mapping from user IDs to preferences
	 * @param fromPriorData
	 *            an implementation detail -- if true, data will map IDs to {@link PreferenceArray} since the framework is attempting to read and update raw data that is already in memory. Otherwise it maps to
	 *            {@link Collection}s of {@link Preference}s, since it's reading fresh data. Subclasses must be prepared to handle this wrinkle.
	 */
	protected void processLine(String line, FastByIDMap<?> data, int userID) {

		// Ignore empty lines and comments
		if (line.isEmpty()) {
			return;
		}

		Iterator<String> tokens = delimiterPattern.split(line).iterator();
		int itemID = 0;
		while (tokens.hasNext()) {
			String next = tokens.next();
			float preferenceValue = Float.parseFloat(next);
			setPreferenceValue(userID, itemID, preferenceValue, data);
			itemID++;
		}

		if (firstLineTokenCount == null) {
			firstLineTokenCount = itemID - 1;
		} else {
			if (itemID - 1 != firstLineTokenCount) {
				throw new IllegalStateException("Number of tokens on line " + userID + " is different from previous lines");
			}
		}

	}

	void setPreferenceValue(Integer userID, Integer itemID, Float preferenceValue, FastByIDMap<?> data) {
		if (Math.abs(preferenceValue - nullPreferenceValue) < 0.001) {
			return;
		}

		Object maybePrefs = data.get(userID);

		// Data are Collection<Preference>
		Collection<Preference> prefs = (Collection<Preference>) maybePrefs;

		boolean exists = false;
		if (prefs != null) {
			for (Preference pref : prefs) {
				if (pref.getItemID().equals(itemID)) {
					exists = true;
					pref.setValue(preferenceValue);
					break;
				}
			}
		}

		if (!exists) {
			if (prefs == null) {
				prefs = Lists.newArrayListWithCapacity(2);
				((FastByIDMap<Collection<Preference>>) data).put(userID, prefs);
			}
			prefs.add(new GenericPreference(userID, itemID, preferenceValue));
		}
	}

	@Override
	public IntPrimitiveIterator getUserIDs() throws TasteException {
		return delegate.getUserIDs();
	}

	@Override
	public PreferenceArray getPreferencesFromUser(Integer userID) throws TasteException {
		return delegate.getPreferencesFromUser(userID);
	}

	@Override
	public FastIDSet getItemIDsFromUser(Integer userID) throws TasteException {
		return delegate.getItemIDsFromUser(userID);
	}

	@Override
	public IntPrimitiveIterator getItemIDs() throws TasteException {
		return delegate.getItemIDs();
	}

	@Override
	public PreferenceArray getPreferencesForItem(Integer itemID) throws TasteException {
		return delegate.getPreferencesForItem(itemID);
	}

	@Override
	public Float getPreferenceValue(Integer userID, Integer itemID) throws TasteException {
		return delegate.getPreferenceValue(userID, itemID);
	}

	@Override
	public Long getPreferenceTime(Integer userID, Integer itemID) throws TasteException {
		return delegate.getPreferenceTime(userID, itemID);
	}

	@Override
	public int getNumItems() throws TasteException {
		return delegate.getNumItems();
	}

	@Override
	public int getNumUsers() throws TasteException {
		return delegate.getNumUsers();
	}

	@Override
	public int getNumUsersWithPreferenceFor(Integer itemID) throws TasteException {
		return delegate.getNumUsersWithPreferenceFor(itemID);
	}

	@Override
	public int getNumUsersWithPreferenceFor(Integer itemID1, Integer itemID2) throws TasteException {
		return delegate.getNumUsersWithPreferenceFor(itemID1, itemID2);
	}

	/**
	 * Note that this method only updates the in-memory preference data that this {@link MatrixDataModel} maintains; it does not modify any data on disk. Therefore any updates from this method are only temporary, and
	 * lost when data is reloaded from a file. This method should also be considered relatively slow.
	 */
	@Override
	public void setPreference(Integer userID, Integer itemID, Float value) throws TasteException {
		delegate.setPreference(userID, itemID, value);
	}

	/** See the warning at {@link #setPreference(long, long, float)}. */
	@Override
	public void removePreference(Integer userID, Integer itemID) throws TasteException {
		delegate.removePreference(userID, itemID);
	}

	@Override
	public void refresh(Collection<Refreshable> alreadyRefreshed) {
		log.debug("File has changed; reloading...");
		reload();
	}

	@Override
	public boolean hasPreferenceValues() {
		return delegate.hasPreferenceValues();
	}

	@Override
	public float getMaxPreference() {
		return delegate.getMaxPreference();
	}

	@Override
	public float getMinPreference() {
		return delegate.getMinPreference();
	}

	@Override
	public String toString() {
		return "FileDataModel[dataFile:" + dataFile + ']';
	}

}
