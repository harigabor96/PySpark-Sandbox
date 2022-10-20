class Conf:

    def __init__(self, master, curated_zone_path, pipeline, raw_zone_path=None, ingest_previous_days=None, terminate_after_ms=None):
        self.master = master
        self.curated_zone_path = curated_zone_path
        self.pipeline = pipeline
        self.raw_zone_path = raw_zone_path
        self.ingest_previous_days = ingest_previous_days
        self.terminate_after_ms = terminate_after_ms
