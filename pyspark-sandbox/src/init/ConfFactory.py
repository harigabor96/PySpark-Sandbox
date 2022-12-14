import argparse

class ConfFactory:

    @staticmethod
    def create_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument('-r', '--raw_zone_path')
        parser.add_argument('-c', '--curated_zone_path')
        parser.add_argument('-p', '--pipeline')
        return parser

    @staticmethod
    def get_conf(args: list[str]) -> argparse.Namespace:
        return \
            ConfFactory \
                .create_parser()\
                .parse_args(args)
