from abc import ABC, abstractmethod


class Heuristic(ABC):

    @abstractmethod
    def compute(self, node) -> int:
        pass


class Zero(Heuristic):
    """Zero heuristic: an admissible heuristic having 0 heruistic value"""
    def compute(self, node) -> int:
        return 0