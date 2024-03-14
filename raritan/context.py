import importlib
import re
import types

import pandas as pd

"""
Provides a context pseudo-singleton to store information about our flow run.
"""


class Context(object):
    """
    A class for storing information about the current flow run.

    This should not be used/instantiated directly in code. See context below.
    This is a class because that reduces the work migrating from Prefect.
    If we want a different structure for this data in the future, we could definitely consider that.
    """

    # A place to stash data during the flow run.
    data_references = {}
    # The active release specification.
    release_spec_name = None
    # The active flow id.
    flow_id = None
    # The current task/step of the flow.
    current_task = None
    # Disable logging/output.
    no_logging = False
    # The namespace to find ETL settings.
    settings_module = 'settings'

    def set_release_spec_name(self, release_spec_name: str) -> None:
        """
        Setter for the release spec name.

        Parameters
        ----------
        release_spec_name: str
            The name of the release spec that is being executed.
        """
        self.release_spec_name = release_spec_name

    def set_flow_id(self, flow_id: str) -> None:
        """
        Setter for the flow id.

        Parameters
        ----------
        flow_id: str
            The name of the flow that is being executed.
        """
        self.flow_id = flow_id

    def set_current_task(self, current_task: str) -> None:
        """
        Setter for the name of the current task.

        Parameters
        ----------
        current_task: str
            The name of the task that is being executed.
        """
        self.current_task = current_task

    def set_no_logging(self, no_logging: bool) -> None:
        """
        Setter for the logging status.

        Parameters
        ----------
        no_logging: str
            Whether the logger produced by core.logger should produce output.
        """
        self.no_logging = no_logging

    def set_data_reference(self, name: str, data_source) -> None:
        """
        Sets an item in the context's list of data references.

        Parameters
        ----------
        name: str
            The name by which to access the data reference.
        data_source
            The data source of mixed type.
        """
        self.data_references[name] = data_source

    def get_data_reference(self, name: str | list):
        """
        Gets a datasource reference or throws if one is not found.

        Parameters
        ----------
        name: str | list
            A name or list of names (or regexes) of data references to get.
        """
        # In the simplest case, get and return a direct string match.
        if type(name) is str:
            if name in self.data_references.keys():
                return self.data_references[name]
            # If we don't find a direct match, see if it's a regex.
            pattern = re.compile(name)
            matches = {}
            for dataset_name, dataset in self.data_references.items():
                if pattern.match(dataset_name):
                    matches[dataset_name] = dataset
            # If we still find no matches, throw.
            message = f'No data sources were named or matched, {name}. Was it included in @import_data?'
            assert len(matches.keys()) > 0, message
            return matches
        if type(name) is list:
            # If we were provided a list, loop over each item and call this method on it.
            matches = {}
            for dataset_name in name:
                data = self.get_data_reference(dataset_name)
                # Respond differently to each return type.
                if type(data) is dict:
                    matches = {** matches, ** data}
                else:
                    matches[dataset_name] = data
            return matches
        # If neither of the above types are found, we don't know how to proceed.
        bad_type = type(name)
        raise RuntimeError(f'Data references may only be gotten by string or list, {bad_type} provided.')

    def print_all_data_references(self):
        """
        Prints information about each data reference in the context, including its key,
        shape (if it's a pandas DataFrame), and its value.
        Parameters
        ----------
        context: dict
            The context containing data references.
        """
        for key, value in context.data_references.items():
            if value is not None:
                if isinstance(value, pd.DataFrame):
                    print(f"Shape of {key}: {value.shape}")
                    print(f"Columns of {key}: {value.columns}")
                print(f"{key}: {value}")

    def clear_data_references(self) -> None:
        """
        Empties the data_references storage.
        """
        self.data_references = {}

    def set_settings_module(self, module_name: str) -> None:
        """
        Sets the ETL settings namespace.

        Parameters
        ----------
        module_name: str
          The module name.
        """
        self.settings_module = module_name

    def get_settings(self) -> types.ModuleType:
        """
        Get the settings module and provide a helpful message, if not found.

        Returns
        -------
        The active settings module.
        """
        try:
            settings = importlib.import_module(self.settings_module)
        except ModuleNotFoundError:
            message = 'Missing required module settings (settings.py). Check out default_settings.py for an example.'
            raise ModuleNotFoundError(message)
        return settings


# The instance of Context that should be manipulated by the system.
context = Context()
