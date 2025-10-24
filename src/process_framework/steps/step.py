from abc import ABC, abstractmethod

class Step(ABC):
    
    @abstractmethod
    def do(self):
        pass
    

    def preflight(self):
        """ perform any preflight assertions; can throw errors or log warnings or do nothing """
        ...