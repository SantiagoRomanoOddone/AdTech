import pandas as pd

def filter_active_advertisers(ads_view_path: str, advertiser_path: str) -> pd.DataFrame:
    """Filter logs for active advertisers."""
    ads_view = pd.read_csv(ads_view_path)
    active_advertisers = pd.read_csv(advertiser_path)
    
    return ads_view[ads_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]

def compute_top_ctr(active_ads_views: pd.DataFrame) -> pd.DataFrame:
    """Compute TopCTR model."""
    active_ads_views['is_click'] = (active_ads_views['type'] == 'click').astype(int)
    ctr = active_ads_views.groupby(['advertiser_id', 'product_id'])['is_click'].mean().reset_index()
    ctr.columns = ['advertiser_id', 'product_id', 'ctr']

    return ctr.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'ctr')).reset_index(drop=True)


if __name__ == '__main__':
    ads_view_path = 'data/ads_views.csv'
    advertiser_path = 'data/advertiser_ids.csv'
    
    active_ads_views = filter_active_advertisers(ads_view_path, advertiser_path)
    top_ctr = compute_top_ctr(active_ads_views)

