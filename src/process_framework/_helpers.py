from process_framework import Step

# Some helper funcs, useful when testing pipeline components in .ipynbs

def perform_preflight(*steps:Step) -> None:
    """Run preflight checks for each step."""
    for step in steps:
        step.preflight()
        

def do(*steps:Step, preflight:bool=False) -> None:
    """Execute steps in order, optionally running all preflight checks first."""
    if preflight:
        perform_preflight(*steps)
        
    for step in steps:
        step.do()