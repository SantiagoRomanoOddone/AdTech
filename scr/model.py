import os
import pandas as pd

def ensure_temp_folder_exists(folder_path: str):
    """Ensure that the temporary folder exists."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

def filter_active_advertiser_views(ads_views_path: str, advertiser_path: str, output_folder: str) -> str:
    """Filter view logs for active advertisers and write to a temporary folder."""
    ads_view = pd.read_csv(ads_views_path)
    active_advertisers = pd.read_csv(advertiser_path)
    
    filtered_views = ads_view[ads_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]
    output_path = os.path.join(output_folder, 'active_ads_views.csv')
    filtered_views.to_csv(output_path, index=False)
    return output_path

def filter_active_advertiser_products(product_views_path: str, advertiser_path: str, output_folder: str) -> str:
    """Filter product logs for active advertisers and write to a temporary folder."""
    product_view = pd.read_csv(product_views_path)
    active_advertisers = pd.read_csv(advertiser_path)
    
    filtered_products = product_view[product_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]
    output_path = os.path.join(output_folder, 'active_product_views.csv')
    filtered_products.to_csv(output_path, index=False)
    return output_path

def compute_top_ctr(active_ads_views_path: str, output_folder: str) -> str:
    """Compute TopCTR model and write results to a temporary folder."""
    active_ads_views = pd.read_csv(active_ads_views_path)
    active_ads_views['is_click'] = (active_ads_views['type'] == 'click').astype(int)
    ctr = active_ads_views.groupby(['advertiser_id', 'product_id'])['is_click'].mean().reset_index()
    ctr.columns = ['advertiser_id', 'product_id', 'ctr']
    
    top_ctr = ctr.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'ctr')).reset_index(drop=True)
    output_path = os.path.join(output_folder, 'top_ctr.csv')
    top_ctr.to_csv(output_path, index=False)
    return output_path

def compute_top_product(active_product_views_path: str, output_folder: str) -> str:
    """Compute TopProduct model and write results to a temporary folder."""
    active_product_views = pd.read_csv(active_product_views_path)
    top_product = active_product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='view_count')
    
    top_products = top_product.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'view_count')).reset_index(drop=True)
    output_path = os.path.join(output_folder, 'top_products.csv')
    top_products.to_csv(output_path, index=False)
    return output_path

# TODO: Implement write_to_db function
def write_to_db():
    """Write results to the database."""


if __name__ == '__main__':
    ads_view_path = 'data/ads_views.csv'
    advertiser_path = 'data/advertiser_ids.csv'
    product_views_path = 'data/product_views.csv'
    
    # Temporary results folder
    temp_folder = 'data/temp'
    ensure_temp_folder_exists(temp_folder)

    # Filter and compute tasks
    active_ads_views_path = filter_active_advertiser_views(ads_view_path, advertiser_path, temp_folder)
    active_product_views_path = filter_active_advertiser_products(product_views_path, advertiser_path, temp_folder)

    top_ctr_path = compute_top_ctr(active_ads_views_path, temp_folder)
    top_products_path = compute_top_product(active_product_views_path, temp_folder)

    print(f"Results saved to temporary folder: {temp_folder}")


