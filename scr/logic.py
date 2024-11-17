import pandas as pd

def filter_active_advertisers(ads_view_path, advertiser_path):
    """Filter logs for active advertisers."""
    ads_view = pd.read_csv(ads_view_path)
    active_advertisers = pd.read_csv(advertiser_path)
    
    return ads_view[ads_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]


if __name__ == '__main__':
    ads_view_path = 'data/ads_views.csv'
    advertiser_path = 'data/advertiser_ids.csv'
    
    active_ads_views = filter_active_advertisers(ads_view_path, advertiser_path)
