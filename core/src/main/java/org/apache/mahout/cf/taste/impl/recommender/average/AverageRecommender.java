package org.apache.mahout.cf.taste.impl.recommender.average;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.AbstractRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

public class AverageRecommender extends AbstractRecommender {

	float averageRating;

	public AverageRecommender(DataModel dataModel) {
		super(dataModel);
		averageRating = (dataModel.getMaxPreference() + dataModel.getMinPreference()) / 2;
	}

	@Override
	public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer rescorer) throws TasteException {
		throw new NotImplementedException("not implemented yet - recommend(userID, howMany, rescorer)");
	}

	@Override
	public float estimatePreference(long userID, long itemID) throws TasteException {
		return averageRating;
	}

	@Override
	public void refresh(Collection<Refreshable> alreadyRefreshed) {
		// TODO Auto-generated method stub
	}

}
