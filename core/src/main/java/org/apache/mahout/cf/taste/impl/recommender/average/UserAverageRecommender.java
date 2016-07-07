package org.apache.mahout.cf.taste.impl.recommender.average;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.AbstractRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

public class UserAverageRecommender extends AbstractRecommender {

	public UserAverageRecommender(DataModel dataModel) {
		super(dataModel);
	}

	@Override
	public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer rescorer) throws TasteException {
		throw new NotImplementedException("not implemented yet - recommend(userID, howMany, rescorer)");
	}

	@Override
	public float estimatePreference(long userID, long itemID) throws TasteException {
		DataModel model = getDataModel();
		PreferenceArray preferencesFromUser = model.getPreferencesFromUser(userID);
		float sumOfPreferences = 0f;
		for (Preference preference : preferencesFromUser) {
			float value = preference.getValue();
			sumOfPreferences += value;
		}
		return sumOfPreferences / preferencesFromUser.length();
	}

	@Override
	public void refresh(Collection<Refreshable> alreadyRefreshed) {
	}

}
