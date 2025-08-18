from abc import ABC, abstractmethod

class Step(ABC):
    
    @abstractmethod
    def do(self):
        pass
    