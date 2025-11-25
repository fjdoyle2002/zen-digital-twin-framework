import pandas as pd
import logging
from retrieval.seeq_retrieval import SeeqRetrieval

logger = logging.getLogger(__name__)


class CoreRetrieval:
    def __init__(self, config, signals_df):
        self.config = config
        self.signals_df = signals_df
        self.retrieval_agents = []
        #this DT implementation was based on Seeq, and while it may not be require, it is the default
        #and usage is currently checked at the initialization of this class
        if self.signals_df['SignalSource'].str.contains('seeq').any():
            self.retrieval_agents.append(SeeqRetrieval(self.config, self.signals_df))

    def add_retrieval_agent(self, agent):
        """
        function adds a retrieval agent to the list of retrieval agents
        retrieval agent must implement the method:
        retrieve_signals_for_actuators_at_timepoint(signals_df, timepoint)  
        """
        self.retrieval_agents.append(agent)

    def retrieve_signals_for_actuators_at_timepoint(self, signals_df, timepoint):
        """
        function calls all applicable retrieval agents to retrieve the physical building's signals of 
        interest that will be used to override EP simulation actuators, and places values in the appropriate dataframes.
        """
        for agent in self.retrieval_agents:
            agent.retrieve_signals_for_actuators_at_timepoint(signals_df, timepoint)


if __name__ == "__main__":
    pass