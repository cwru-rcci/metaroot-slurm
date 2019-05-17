from metaroot.rpc.client import RPCClient
from metaroot.config import get_config
from metaroot.api.result import Result


class SlurmManager(RPCClient):
    """
    RPC wrapper for metaroot.slurm.manager.SlurmManager
    Auto-generated 2019-03-18T10:09:36.029748
    """
    def __init__(self):
        super().__init__(get_config(self.__class__.__name__))

    def __enter__(self):
        """
        Connect the RPC client to the message queue if manager is instantiated by a with statement
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Disconnects the RPC client from the message queue when with statement block is exited
        """
        self.close()

    def initialize(self):
        """
        Connects the RPC client to the message queue
        """
        self.connect()

    def finalize(self):
        """
        Disconnects the RPC client fomr the message queue
        """
        self.close()

    def add_group(self, group_atts) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.add_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.add_group
        """
        request = {'action': 'add_group',
                   'group_atts': group_atts,
                   }
        return self.send(request)

    def add_user(self, user_atts) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.add_user

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.add_user
        """
        request = {'action': 'add_user',
                   'user_atts': user_atts,
                   }
        return self.send(request)

    def associate_user_to_group(self, user_name, group_name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.associate_user_to_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.associate_user_to_group
        """
        request = {'action': 'associate_user_to_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self.send(request)

    def delete_group(self, name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.delete_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.delete_group
        """
        request = {'action': 'delete_group',
                   'name': name,
                   }
        return self.send(request)

    def delete_user(self, name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.delete_user

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.delete_user
        """
        request = {'action': 'delete_user',
                   'name': name,
                   }
        return self.send(request)

    def disassociate_user_from_group(self, user_name, group_name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.disassociate_user_from_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.disassociate_user_from_group
        """
        request = {'action': 'disassociate_user_from_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self.send(request)

    def disassociate_users_from_group(self, user_names, group_name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.disassociate_users_from_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.disassociate_users_from_group
        """
        request = {'action': 'disassociate_users_from_group',
                   'user_names': user_names,
                   'group_name': group_name,
                   }
        return self.send(request)

    def exists_group(self, name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.exists_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.exists_group
        """
        request = {'action': 'exists_group',
                   'name': name,
                   }
        return self.send(request)

    def exists_user(self, name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.exists_user

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.exists_user
        """
        request = {'action': 'exists_user',
                   'name': name,
                   }
        return self.send(request)

    def get_group(self, name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.get_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.get_group
        """
        request = {'action': 'get_group',
                   'name': name,
                   }
        return self.send(request)

    def get_members(self, name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.get_members

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.get_members
        """
        request = {'action': 'get_members',
                   'name': name,
                   }
        return self.send(request)

    def get_user(self, name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.get_user

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.get_user
        """
        request = {'action': 'get_user',
                   'name': name,
                   }
        return self.send(request)

    def set_user_default_group(self, user_name, group_name) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.set_user_default_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.set_user_default_group
        """
        request = {'action': 'set_user_default_group',
                   'user_name': user_name,
                   'group_name': group_name,
                   }
        return self.send(request)

    def list_groups(self) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.list_groups

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.list_groups
        """
        request = {'action': 'list_groups'}
        return self.send(request)

    def list_users(self) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.list_users

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.list_users
        """
        request = {'action': 'list_users'}
        return self.send(request)

    def update_group(self, group_atts) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.update_group

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.update_group
        """
        request = {'action': 'update_group',
                   'group_atts': group_atts,
                   }
        return self.send(request)

    def update_user(self, user_atts) -> Result:
        """
        An RPC wrapper to the method metaroot.slurm.manager.SlurmManager.update_user

        See Also
        --------
        metaroot.slurm.manager.SlurmManager.update_user
        """
        request = {'action': 'update_user',
                   'user_atts': user_atts,
                   }
        return self.send(request)

