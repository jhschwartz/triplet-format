from torch.utils.data.dataset import T_co

from triplets_format import BigSmallFormat
from torch.utils.data import IterableDataset, get_worker_info


class TripletDataset(IterableDataset):
    def __init__(self, start, end):
        IterableDataset.__init__(self)
        assert end > start
        self.start = start
        self.end = end


    def __iter__(self):
        worker_info = get_worker_info
        if worker_info is None:
            # single process
            return iter(range(self.start, self.end))
        per_worker = int(math.ceil((self.end - self.start) / float(worker_info.num_workers)))
        worker_id = worker_info.id
        iter_start = self.start + worker_id * per_worker
        iter_end = min(iter_start + per_worker, self.end)
        return iter(range(iter_start, iter_end))
