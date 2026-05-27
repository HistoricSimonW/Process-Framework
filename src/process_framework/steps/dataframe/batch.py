

@dataclass
class BatchProcessDataFrame(BatchProcessor[DataFrame, DataFrame]):

    def gen_batches(self, subject: DataFrame) -> Iterable[DataFrame]:
            n = len(subject.index)
            for start in range(0, n, self.batch_size):
                yield subject.iloc[start : start + self.batch_size, :].copy()