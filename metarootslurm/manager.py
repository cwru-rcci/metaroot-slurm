import subprocess
from metaroot.utils import get_logger
from metaroot.config import get_config
from metaroot.api.result import Result

# Defines a default schema to use if none is specified
default_slurm_account_schema = {
    "Organization": True,
    "Cluster": True,
    "DefaultQOS": True,
    "Fairshare": True,
    "GrpTRESMins": True,
    "GrpTRESRunMins": True,
    "GrpTRES": True,
    "GrpJobs": True,
    "GrpJobsAccrue": False,
    "GrpSubmitJobs": True,
    "GrpWall": True,
    "GrpCPUS": True,
    "ID": True,
    "LFT": True,
    "MaxTRESMins": True,
    "MaxTRES": True,
    "MaxJobs": True,
    "MaxJobsAccrue": False,
    "MaxSubmitJobs": True,
    "MaxWall": True,
    "Qos": True,
    "ParentID": True,
    "ParentName": True,
    "Partition": True,
    "WithRawQOSLevel": False,
    "RGT": True}


class SlurmAccount:
    """
    Encapsulates the properties of a SLURM Account where the string format of the object coincides with the command
    line syntax expected by sacctmgr. E.g. relative to obj, "sacctmgr add account {0}".format(obj) will be a valid
    command that can be executed by the shell. If constructor is passed an invalid attribute, raises an exception.
    """

    def __init__(self, attrs: dict, valid_keys: dict):
        """
        Override default init to accept a name (account name) and dictionary of attributes

        Parameters
        ----------
        attrs : dict
            Attributes related to the account. Must all be defined in SlurmAccount.valid_keys

        Raises
        ---------
        Exception
            If any keys in the argument attributes are not members of SlurmAccount.valid_keys
        """
        if "name" in attrs:
            self._name = attrs["name"]
        self._attr = {}

        for key in attrs:
            if key == "name":
                pass
            elif key in valid_keys and valid_keys[key] == 1:
                self._attr[key] = str(attrs[key])
            elif key in valid_keys and valid_keys[key] == 0:
                pass
            else:
                raise Exception("Invalid attribute key specified for Account: "+key)

    def __str__(self):
        """
        Default string representation can be passed to SLURM add account command

        Returns
        ----------
        str
            The account as a string that can be passed to SLURM add command, e.g. name=foo att1=val att2=val ...
        """
        r = "name="+self._name
        for key in self._attr:
            r += " " + key + "=" + self._attr[key]
        return r

    def as_update_str(self):
        """
        Format the account as a string that can be passed to SLURM modify commands

        Returns
        ----------
        str
            The account as a string that can be passed to SLURM modify command, e.g. WHERE name=foo SET att1=val ...
        """
        r = "where name=" + self._name + " set"
        for key in self._attr:
            r += " " + key + "=" + self._attr[key]
        return r

    def name(self):
        """
        Convenience function to return the slurm account name

        Returns
        ----------
        str
            The account name
        """
        return self._name

    def format_string(self):
        """
        Creates a string to pass as the format=string parameter of SLURM list commands. It incorporates all attributes
        defined as valid keys so that all columns with defined values will be output.

        Returns
        ----------
        str
            A string that specifies the columns to output
        """
        fmt_string = ""
        for key in self._attr:
            if len(fmt_string) > 0:
                fmt_string = fmt_string + ","
            fmt_string = fmt_string + key
        return fmt_string


class SlurmUser:
    """
    Encapsulates the properties of a SLURM User where the string format of the object coincides with the command
    line syntax expected by sacctmgr. E.g. relative to obj, "sacctmgr add user {0}".format(obj) will be a valid
    command that can be executed by the shell. If constructor is passed an invalid attribute, raises an exception.
    """

    # Define a set of optional valid attributes that we allow to be associated with a SlurmUser instance
    valid_keys = {"DefaultAccount": True,
                  "Account": True,
                  "MaxJobs": True}

    def __init__(self, attrs: dict):
        """
        Override default init to accept a name (user name) and dictionary of attributes

        Parameters
        ----------
        attrs : dict
            Attributes related to the user

        Raises
        ---------
        Exception
            If any keys in the argument attributes are not members of SlurmUser.valid_keys
        """
        self._name = attrs["name"]
        self._attr = {}

        for key in attrs:
            if key == "name":
                continue
            elif key in SlurmUser.valid_keys:
                self._attr[key] = str(attrs[key])
            else:
                raise Exception("Invalid attribute key specified for User: "+key)

    def __str__(self):
        """
        Default string representation can be passed to SLURM add user command

        Returns
        ----------
        str
            The user formatted into a string that can be passed to SLURM add command, e.g. name=foo defaultaccount=bar
        """
        r = "name="+self._name
        for key in self._attr:
            r += " " + key + "=" + self._attr[key]
        return r

    def as_update_str(self):
        """
        Format the user as a string that can be passed to SLURM modify commands

        Returns
        ----------
        str
            The user formatted into a string that can be passed to SLURM modify command, e.g. WHERE name=foo SET att=val...
        """

        # Build the "WHERE name=${username} [AND account=${accountname}] SET..." portion of command
        r = "where name=" + self._name
        if 'Account' in self._attr:
            r = r + " account="+self._attr['Account']
        r = r + " set"

        for key in self._attr:
            # Don't add Account in the set portion of the command if it is present
            if key == 'Account':
                continue
            r += " " + key + "=" + self._attr[key]
        return r

    def name(self):
        """
        Convenience function to return the SLURM user name

        Returns
        ----------
        str
            The user name
        """
        return self._name


class SlurmManager:
    """
    Implements methods to manage users and accounts in SLURM via the sacctmgr command.
    """

    def __init__(self):
        """
        Load configuration and setup logging
        """
        try:
            config = get_config(self.__class__.__name__)
        except Exception as e:
            config = None

        if config is not None:
            self._logger = get_logger(self.__class__.__name__,
                                      config.get_log_file(),
                                      config.get_mq_file_verbosity(),
                                      config.get_mq_screen_verbosity())

            if config.has("METAROOT_SACCTMGR_PATH"):
                self._sacctmgr = config.get("METAROOT_SACCTMGR_PATH")
            else:
                self._logger.warning("The configuration does not specify a path to sacctmgr via the key " +
                                     "METAROOT_SACCTMGR_PATH. Will use the default path /usr/bin/sacctmgr")
                self._sacctmgr = "/usr/bin/sacctmgr"

            if config.has("METAROOT_SACCT_SCHEMA"):
                self._schema = config.get("METAROOT_SACCT_SCHEMA")
            else:
                self._logger.warning("The configuration does not specify a SLURM account schema via the key " +
                                     "METAROOT_SACCT_SCHEMA. Will use the default schema")
                self._schema = default_slurm_account_schema

        else:
            print("**************************************************************")
            print("The configuration file metaroot[-test].yaml could not be found")
            print("Using default parameters and logging at debug level to screen")
            print("***************************************************************")
            self._logger = get_logger(self.__class__.__name__,
                                      "$NONE",
                                      "CRITICAL",
                                      "DEBUG")
            self._sacctmgr = "/usr/bin/sacctmgr"
            self._schema = default_slurm_account_schema

    # Runs the argument command and returns the exist status. Attempts to suppress all output.
    def __run_cmd__(self, cmd: str):
        args = cmd.split()
        try:
            cp = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            self._logger.error("The command could not be run")
            self._logger.exception(e)
            return 456

        if cp.returncode != 0:
            self._logger.error("CMD: {0}".format("\"{0}\"".format(cmd)))
            self._logger.error("STDOUT: {0}".format(cp.stdout.decode("utf-8")))
            self._logger.error("STDERR: {0}".format(cp.stderr.decode("utf-8")))
            self._logger.error("Command failed with exit status %d", cp.returncode)
        else:
            self._logger.debug("\"{0}\" returned {1}".format(cmd, cp.returncode))

        return cp.returncode

    # Runs the argument command and returns the output as a string. returns None if the command did not exit with
    # status 0
    def __run_cmd2__(self, cmd: str):
        args = cmd.split()

        try:
            cp = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            self._logger.error("The command could not be run")
            self._logger.exception(e)
            return None

        if cp.returncode != 0:
            self._logger.error("CMD: %s", cmd)
            self._logger.error("STDOUT: {0}".format(cp.stdout.decode("utf-8")))
            self._logger.error("STDERR: {0}".format(cp.stderr.decode("utf-8")))
            self._logger.error("Command failed with exit status %d", cp.returncode)
            return None
        else:
            self._logger.debug("\"{0}\" returned {1}".format(cmd, cp.returncode))
            return cp.stdout.decode("utf-8")

    def add_group(self, group_atts: dict) -> Result:
        """
        Add a new SLURM account

        Parameters
        ----------
        group_atts : dict
            Properties defining the SLURM account

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 if the operation failed (e.g., if the account already exists)
        """
        self._logger.info("add_account {0}".format(group_atts))
        account = SlurmAccount(group_atts, self._schema)

        exists_group = self.exists_group(account.name())
        if exists_group.is_error():
            return Result(1, "Error checking for existence of group")
        elif exists_group.response is True:
            return Result(0, "Group already exists")

        cmd = self._sacctmgr+" -i -Q create account {0}".format(account)
        status = self.__run_cmd__(cmd)
        return Result(status, None)

    def get_group(self, name: str) -> Result:
        """
        Retrieve the current configuration of an account

        Parameters
        ----------
        name : str
            SLURM account name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is a dictionary of key=value pairs corresponding to SLURM configuration parameters. If the
            operation fails, an empty dictionary is returned.
        """
        self._logger.info("get_account {0}".format(name))

        group_atts = {"name": name}
        group_atts.update(self._schema)
        account = SlurmAccount(group_atts, self._schema)

        cmd = self._sacctmgr+" -P list account WithAssoc name='" + name + "' user='' format='Account,"+account.format_string()+"'"
        stdout = self.__run_cmd2__(cmd)
        account = {}

        if stdout is None:
            self._logger.error("Command %s requested STDOUT but returned None", cmd)
        else:
            lines = stdout.splitlines()
            if len(lines) is not 2:
                return Result(1, account)

            header_tokens = lines[0].split('|')
            data_tokens = lines[1].split('|')
            for i in range(len(header_tokens)):
                account[header_tokens[i]] = data_tokens[i]

            # Add group members to returned object
            account["memberUid"] = []
            member_result = self.get_members(name)
            if member_result.is_success():
                account["memberUid"] = member_result.response

            self._logger.debug(account)

        return Result(0, account)

    def list_groups(self):
        """
        Retrieve the names of all SLURM accounts in the database

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is an array of account names defined in the database
        """
        self._logger.info("list_groups")

        cmd = self._sacctmgr + " -P list account"
        stdout = self.__run_cmd2__(cmd)
        accounts = []

        if stdout is None:
            self._logger.error("Command %s requested STDOUT but returned None", cmd)
            return Result(1, accounts)
        else:
            lines = stdout.splitlines()
            for i in range(1, len(lines)):
                if lines[i] is not '':
                    tokens = lines[i].split("|")
                    accounts.append(tokens[0])

            self._logger.debug(accounts)

        return Result(0, accounts)

    def get_members(self, name: str) -> Result:
        """
        Retrieve the user names of all users that are associated with the account

        Parameters
        ----------
        name : str
            SLURM account name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is an array of user names associated with the SLURM account. If the operation fails, an
            empty array is returned
        """
        self._logger.info("get_members {0}".format(name))

        cmd = self._sacctmgr+" -P list account WithAssoc name='" + name + "' format='User'"
        stdout = self.__run_cmd2__(cmd)
        members = []

        if stdout is None:
            self._logger.error("Command %s requested STDOUT but returned None", cmd)
            return Result(1, members)
        else:
            lines = stdout.splitlines()
            for i in range(1, len(lines)):
                if lines[i] is not '':
                    members.append(lines[i])

            self._logger.debug(members)

        return Result(0, members)

    def update_group(self, group_atts: dict) -> Result:
        """
        Change the configuration of a SLURM account

        Parameters
        ----------
        group_atts: dict
            Properties defining a SLURM account to update. The name of the argument account must match the name of the
            account to update in the SLURM database.

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
        """
        self._logger.info("update_account {0}".format(group_atts))

        account = SlurmAccount(group_atts, self._schema)
        cmd = self._sacctmgr+" -i modify account {0}".format(account.as_update_str())
        status = self.__run_cmd__(cmd)
        return Result(status, None)

    def delete_group(self, name: str) -> Result:
        """
        Delete an account from SLURM. This operation manages migrating user default accounts away from the account
        to be deleted prior to attempting the delete operation.

        Parameters
        ----------
        name : str
            SLURM account name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
        """
        self._logger.info("delete_account {0}".format(name))

        # If group does not exist, return success but provide informational message
        exists_group = self.exists_group(name)
        if exists_group.is_error():
            return Result(1, None)
        elif exists_group.response is False:
            return Result(0, "Group does not exist")

        # Get current account members
        get_members = self.get_members(name)
        if get_members.is_error():
            return Result(1, None)

        # Remove users linked to the account first. This is a complex operation because it has to change primary
        # user account affiliations before removing the account below
        remove_members = self.disassociate_users_from_group(get_members.response, name)
        if remove_members.is_error():
            return Result(2, None)

        # Remove the account
        cmd = self._sacctmgr+" -i delete account name="+name
        status = self.__run_cmd__(cmd)
        return Result(status, None)

    def exists_group(self, name: str) -> Result:
        """
        Test if a SLURM account exists with a specified name

        Parameters
        ----------
        name : str
            Name to check against SLURM account names

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is True for exists, False for does not exist
        """
        self._logger.info("exists_account {0}".format(name))

        cmd = self._sacctmgr+" -n list account name=" + name
        stdout = self.__run_cmd2__(cmd)
        if stdout is not None:
            if len(stdout.splitlines()) == 1:
                return Result(0, True)
            else:
                return Result(0, False)
        return Result(1, False)

    def add_user(self, user_atts: dict) -> Result:
        """
        Add a new SLURM user. If the user already exists it is no overwritten.

        Parameters
        ----------
        user_atts: dict
            Properties defining a SLURM user

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error

        See Also
        --------
        update_user
        """
        self._logger.info("add_user {0}".format(user_atts))
        user = SlurmUser(user_atts)

        # If the user already exists, do nothing
        result = self.exists_user(user.name())
        if result.is_error():
            return Result(1, "Error testing user exists")
        elif result.response is True:
            return Result(0, "User already exists")

        # Otherwise, add the user
        cmd = self._sacctmgr+" -i create user {0}".format(user)
        status = self.__run_cmd__(cmd)
        return Result(status, None)

    def update_user(self, user_atts: dict) -> Result:
        """
        Change the configuration of a SLURM user. The argument user object must contain an "Account" attribute, as this
        method is only applicable to changing user parameters associated with an account.

        Parameters
        ----------
        user_atts : dict
            Properties defining a SLURM user. The name of the argument user must match the name of the user to update in
            the SLURM database.

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
        """
        self._logger.info("update_user {0}".format(user_atts))

        user = SlurmUser(user_atts)
        cmd = self._sacctmgr+" -i modify user {0}".format(user.as_update_str())
        status = self.__run_cmd__(cmd)
        return Result(status, None)

    def get_user(self, name: str) -> Result:
        """
        Retrieve the current configuration of a user

        Parameters
        ----------
        name : str
            SLURM user name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is a nested data structure {'default': str, 'account_name1': {}, 'account_name2': {} ... }
            where default is the account name of the user's configured default and the nested dictionaries are the
            user's configured attributes associated with each named account. If the operation fails, an empty dictionary
            will be returned.
        """
        self._logger.info("get_user {0}".format(name))
        account = SlurmAccount(self._schema, self._schema)
        cmd = self._sacctmgr+" -P list user WithAssoc name='" + name + "' format='Account,DefaultAccount," + account.format_string() + "'"
        stdout = self.__run_cmd2__(cmd)
        user = {}

        if stdout is None:
            self._logger.error("Command %s requested STDOUT but returned None", cmd)
            return Result(1, user)
        else:
            lines = stdout.splitlines()
            header = lines[0]
            header_tokens = header.split('|')

            for j in range(1, len(lines)):
                data_tokens = lines[j].split('|')
                account = {}
                user['default'] = data_tokens[1]
                for i in range(len(header_tokens)):
                    account[header_tokens[i]] = data_tokens[i]
                user[data_tokens[0]] = account
            self._logger.debug(user)
        return Result(0, user)

    def list_users(self):
        """
        Retrieve the names of all SLURM users in the database

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is an array of user names defined in the database
        """
        self._logger.info("list_users")

        cmd = self._sacctmgr + " -P list user"
        stdout = self.__run_cmd2__(cmd)
        users = []

        if stdout is None:
            self._logger.error("Command %s requested STDOUT but returned None", cmd)
            return Result(1, users)
        else:
            lines = stdout.splitlines()
            for i in range(1, len(lines)):
                if lines[i] is not '':
                    tokens = lines[i].split("|")
                    users.append(tokens[0])

            self._logger.debug(users)

        return Result(0, users)

    def delete_user(self, name: str) -> Result:
        """
        Delete a user from SLURM.

        Parameters
        ----------
        name : str
            SLURM user name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
        """
        self._logger.info("delete_user {0}".format(name))

        exists_user = self.exists_user(name)
        if exists_user.is_error():
            return Result(1, "Error testing user exists")
        elif exists_user.response is False:
            return Result(0, "User does not exist")

        cmd = self._sacctmgr+" -i delete user name="+name
        status = self.__run_cmd__(cmd)
        return Result(status, None)

    def exists_user(self, name: str) -> Result:
        """
        Test if a user exists with a specified name

        Parameters
        ----------
        name : str
            SLURM user name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is True for exists, False for does not exist
        """
        self._logger.info("exists_user {0}".format(name))

        cmd = self._sacctmgr+" -n list user name=" + name
        stdout = self.__run_cmd2__(cmd)
        if stdout is not None:
            if len(stdout.splitlines()) == 1:
                self._logger.debug("User %s does exist", name)
                return Result(0, True)
            else:
                self._logger.debug("User %s does NOT exist", name)
                return Result(0, False)
        return Result(1, "Command Error")

    def set_user_default_group(self, user_name: str, group_name: str) -> Result:
        """
        Set a user's default account affiliation

        Parameters
        ----------
        user_name : str
            SLURM user name
        group_name : str
            SLURM account name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
        """
        self._logger.info("set_user_default_account {0}, {1}".format(user_name, group_name))

        cmd = self._sacctmgr+" -i modify user where name=" + user_name + " set defaultaccount=" + group_name
        status = self.__run_cmd__(cmd)
        return Result(status, None)

    def associate_user_to_group(self, user_name: str, group_name: str) -> Result:
        """
        Associate an account with a user (grant membership)

        Parameters
        ----------
        user_name : str
            SLURM user name
        group_name : str
            SLURM account name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
        """
        self._logger.info("associate_user_to_account {0} {1}".format(user_name, group_name))

        cmd = self._sacctmgr+" -i add user name='" + user_name + "' account='" + group_name + "'"
        status = self.__run_cmd__(cmd)
        return Result(status, None)

    def disassociate_user_from_group(self, user_name: str, group_name: str) -> Result:
        """
        Remove a user's association with an account (revoke membership)

        Parameters
        ----------
        user_name : str
            SLURM user name
        group_name : str
            SLURM account name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is True if user had their default group set to "bench", False otherwise
        """
        self._logger.info("disassociate_user_from_account {0}, {1}".format(user_name, group_name))

        result = self.get_user(user_name)
        benched = False

        # Check if non-existent user name specified
        if 'default' not in result.response:
            self._logger.warn("attempt to disassociate user %s that does not exist", user_name)

        # If we are trying to remove the primary group affiliation of the user, set their primary affiliation to
        # the special reserve group 'bench', in which case the user will need to select a new default account for
        # themself
        elif result.response['default'] == group_name:
            # This can fail if the user already has an association the the bench account
            self.associate_user_to_group(user_name, 'bench')

            # Move the user to the bench account
            benched = self.set_user_default_group(user_name, 'bench').is_success()
            if benched:
                self._logger.warn("disassociate_user_from_account {0}, {1} -> User was benched".format(user_name, group_name))

        # Remove the user affiliation
        cmd = self._sacctmgr+" -i delete user name='" + user_name + "' account='" + group_name + "'"
        status = self.__run_cmd__(cmd)
        return Result(status, benched)

    def disassociate_users_from_group(self, user_names: list, group_name: str) -> Result:
        """
        Remove a user's association with an account (revoke membership)

        Parameters
        ----------
        user_names : list
            SLURM user names
        group_name : str
            SLURM account name

        Returns
        ---------
        Result
            Result.status is 0 for success, >0 on error
            Result.response is a list of user names that had their default account set to "bench" by the operation
        """
        self._logger.info("disassociate_users_from_account {0}, {1}".format(user_names, group_name))

        global_status = 0
        affected = []
        for user_name in user_names:
            result = self.disassociate_user_from_group(user_name, group_name)
            global_status = global_status + result.status
            if result.response:
                affected.append(user_name)
        return Result(global_status, affected)
