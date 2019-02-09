from pricing import PriceManager
import click
import yaml


@click.command()
@click.option('--filter_file', help='Path to yaml filter file')
def main(filter_file):
    filters = {}
    if filter_file:
        with open(filter_file) as f:
            filters = yaml.safe_load(f)
    pm = PriceManager(filters=filters)
    pm.download_prices()
    pm.upload_files()
    pm.create_tables()
    pm.load_partitions()


if __name__ == '__main__':
    main()
