"""The smallest leaf job: a figures service that stores the regions it is given. The reference for a new service."""

from evilflowers_worker.contract import Region, RegionsJob
from evilflowers_worker.jobs import LeafJob


class EchoFigures(LeafJob[RegionsJob, list[Region]]):
    kind = "figures"

    def extract(self) -> list[Region]:
        found = []
        for number, region in enumerate(self.message.regions):
            if number and number % 100 == 0:
                self.recheck()  # a long job asks between batches whether the run still wants it
            found.append(Region(kind="figure", page=region.page, seq=region.seq, bbox=region.bbox))
        return found

    def publish(self, result: list[Region]) -> None:
        self.catalog.regions(result)

    def provenance(self) -> dict:
        return {**super().provenance(), "extractor": "echo"}
