from prefect import flow, task
from core.scrapers import OutNetScraper

@task
def task_scrape_outnet():
    scraper = OutNetScraper(project_name='Luxury-Discount-Tracking', scraper_name='OutNetScraper')
    scraper.scrape()

@flow(log_prints=True)
def discount_tracking():
    task_scrape_outnet()

