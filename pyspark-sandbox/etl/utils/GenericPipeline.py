from pyspark.sql import DataFrame
from abc import ABC, abstractmethod

class GenericPipeline(ABC):

    @abstractmethod
    def execute(self) -> None:
        pass

    @abstractmethod
    def _extract(self) -> DataFrame:
        pass

    @abstractmethod
    def _transform(self, extracted_df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def _load(self, transformed_df: DataFrame) -> None:
        pass
