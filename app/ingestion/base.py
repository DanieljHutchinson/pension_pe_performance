from abc import ABC, abstractmethod

from app.models import RawRecord


class BaseScraper(ABC):
    pension_fund: str
    source_url: str

    @abstractmethod
    def fetch(self) -> None:
        """Download or retrieve raw data from the source."""

    @abstractmethod
    def parse(self) -> list[RawRecord]:
        """Parse fetched data into raw records."""

    def run(self) -> list[RawRecord]:
        self.fetch()
        return self.parse()
