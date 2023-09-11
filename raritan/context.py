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

    def __init__(self):
        self.data_references = {}
        self.release_spec_name = None
        self.flow_id = None
        self.current_task = None
        self.no_logging = False

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

    def get_data_reference(self, name):
        """
        Gets a datasource reference or throws if one is not found.

        Parameters
        ----------
        name: str
            The name by which to access the data reference.
        """
        if name not in self.data_references.keys():
            raise RuntimeError(f'Requested unloaded datasource, {name}. Was it included in @import_data?')
        return self.data_references[name]


# The instance of Context that should be manipulated by the system.
context = Context()
