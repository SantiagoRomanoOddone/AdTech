import pandas as pd

def filter_active_advertiser_views(ads_views_path: str, advertiser_path: str) -> pd.DataFrame:
    """Filter view logs for active advertisers."""
    ads_view = pd.read_csv(ads_views_path)
    active_advertisers = pd.read_csv(advertiser_path)
    
    return ads_view[ads_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]

def filter_active_advertiser_products(product_views_path: str, advertiser_path: str) -> pd.DataFrame:
    """Filter product logs for active advertisers."""
    product_view = pd.read_csv(product_views_path)
    active_advertisers = pd.read_csv(advertiser_path)
    
    return product_view[product_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]

def compute_top_ctr(active_ads_views: pd.DataFrame) -> pd.DataFrame:
    """Compute TopCTR model."""
    active_ads_views['is_click'] = (active_ads_views['type'] == 'click').astype(int)
    ctr = active_ads_views.groupby(['advertiser_id', 'product_id'])['is_click'].mean().reset_index()
    ctr.columns = ['advertiser_id', 'product_id', 'ctr']

    return ctr.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'ctr')).reset_index(drop=True)

def compute_top_product(active_product_views: pd.DataFrame) -> pd.DataFrame:
    """Compute TopProduct model."""
    top_product = active_product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='view_count')

    return top_product.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'view_count')).reset_index(drop=True)

# TODO: Implement write_to_db function
def write_to_db():
    """Write results to the database."""


if __name__ == '__main__':
    pass
    # ads_view_path = 'data/ads_views.csv'
    # advertiser_path = 'data/advertiser_ids.csv'
    # product_views_path = 'data/product_views.csv'
    
    # active_ads_views = filter_active_advertiser_views(ads_view_path, advertiser_path)
    # active_product_views = filter_active_advertiser_products(product_views_path, advertiser_path)

    # top_ctr = compute_top_ctr(active_ads_views)
    # top_product = compute_top_product(active_product_views)


