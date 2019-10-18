import uuid
import pydoc
import inspect
import itertools
import pkg_resources
from enum import Enum
from dataclasses import dataclass, field
from typing import (
    NamedTuple,
    Union,
    List,
    Dict,
    Optional,
    Any,
    Iterator,
    Callable,
)

from ..base import BaseConfig
from ..util.data import export_dict
from ..util.entrypoint import Entrypoint, base_entry_point


class Definition(NamedTuple):
    """
    List[type] is how to specify a list
    """

    name: str
    primitive: str
    lock: bool = False
    # spec is a NamedTuple which could be populated via a dict
    spec: NamedTuple = None

    def __repr__(self):
        return self.name

    def __str__(self):
        return repr(self)

    def export(self):
        exported = dict(self._asdict())
        if not self.lock:
            del exported["lock"]
        if not self.spec:
            del exported["spec"]
        else:
            exported["spec"] = {
                "name": exported["spec"].__qualname__,
                "types": exported["spec"]._field_types,
                "defaults": exported["spec"]._field_defaults,
            }
        return exported

    @classmethod
    def _fromdict(cls, **kwargs):
        if "spec" in kwargs:
            # Alright this is horrible. But bear with me here. The
            # typing.NamedTuple API as of 3.7 does not provide a clean way to
            # create a new NamedTuple class where you specify the type hinting
            # and the default values. The following is based on looking at the
            # soruce code
            # https://github.com/python/cpython/blob/3.7/Lib/typing.py#L1360
            # and seeing that we can hijack the __annotations__ property to
            # allow us to set default values
            def_tuple = kwargs["spec"]["types"]
            def_tuple["__annotations__"] = kwargs["spec"]["defaults"]
            kwargs["spec"] = type(
                kwargs["spec"]["name"], (NamedTuple,), def_tuple
            )
        return cls(**kwargs)

    @classmethod
    def type_lookup(cls, typename):
        # Allowlist of non-python builtin types
        if typename in ["Definition"]:
            # TODO More types
            return cls
        # TODO(security) Make sure this won't blow up in our face ever
        return pydoc.locate(typename)


class Stage(Enum):
    PROCESSING = "processing"
    CLEANUP = "cleanup"
    OUTPUT = "output"


class FailedToLoadOperation(Exception):
    """
    Raised when an Operation wasn't found to be registered with the
    dffml.operation entrypoint.
    """


@base_entry_point("dffml.operation", "operation")
class Operation(NamedTuple, Entrypoint):
    name: str
    inputs: Dict[str, Definition]
    outputs: Dict[str, Definition]
    stage: Stage = Stage.PROCESSING
    conditions: Optional[List[Definition]] = []
    expand: Optional[List[str]] = []
    instance_name: Optional[str] = None

    def export(self):
        exported = {
            "name": self.name,
            "inputs": self.inputs.copy(),
            "outputs": self.outputs.copy(),
            "conditions": self.conditions.copy(),
            "stage": self.stage.value,
            "expand": self.expand.copy(),
        }
        for to_string in ["conditions"]:
            exported[to_string] = list(
                map(lambda definition: definition.name, exported[to_string])
            )
        for to_string in ["inputs", "outputs"]:
            exported[to_string] = dict(
                map(
                    lambda key_def: (key_def[0], key_def[1].export()),
                    exported[to_string].items(),
                )
            )
        if not exported["conditions"]:
            del exported["conditions"]
        if not exported["expand"]:
            del exported["expand"]
        return exported

    @classmethod
    def definitions(cls, *args: "Operation"):
        """
        Create key value mapping of definition names to definitions for all
        given operations.
        """
        definitions = {}
        for op in args:
            for has_definition in ["inputs", "outputs"]:
                for definition in getattr(op, has_definition, {}).values():
                    definitions[definition.name] = definition
            for has_definition in ["conditions"]:
                for definition in getattr(op, has_definition, []):
                    definitions[definition.name] = definition
        return definitions

    @classmethod
    def load(cls, loading=None):
        loading_classes = []
        # Load operations
        for i in pkg_resources.iter_entry_points(cls.ENTRY_POINT):
            if loading is not None and i.name == loading:
                loaded = i.load()
                if isinstance(loaded, cls):
                    return loaded
                elif isinstance(getattr(loaded, "op", None), cls):
                    # Handle operations decorated with op
                    return loaded.op
            else:
                loaded = i.load()
                loading_classes.append(loaded)
        for i in pkg_resources.iter_entry_points(cls.ENTRY_POINT):
            if loading is not None and i.name == loading:
                return i.load()
            else:
                loading_classes.append(loaded)
        if loading is not None:
            raise KeyError(
                "%s was not found in (%s)"
                % (
                    repr(loading),
                    ", ".join(list(map(lambda op: op.name, loading_classes))),
                )
            )
        return loading_classes

    @classmethod
    def _op(cls, loaded):
        """
        Returns the operation from a loaded entrypoint object, or None if its
        not an operation or doesn't have the op parameter which is an operation.
        """
        for obj in [loaded, getattr(loaded, "op", None)]:
            if isinstance(obj, cls):
                return obj
        return None

    @classmethod
    def load(cls, loading=None):
        loading_classes = []
        # Load operations
        for i in pkg_resources.iter_entry_points(cls.ENTRY_POINT):
            if loading is not None and i.name == loading:
                loaded = cls._op(i.load())
                if loaded is not None:
                    return loaded
            elif loading is None:
                loaded = cls._op(i.load())
                if loaded is not None:
                    loading_classes.append(loaded)
        if loading is not None:
            raise FailedToLoadOperation(
                "%s was not found in (%s)"
                % (
                    repr(loading),
                    ", ".join(list(map(lambda op: op.name, loading_classes))),
                )
            )
        return loading_classes

    @classmethod
    def _fromdict(cls, **kwargs):
        for prop in ["inputs", "outputs"]:
            kwargs[prop] = {
                argument_name: Definition._fromdict(**definition)
                for argument_name, definition in kwargs[prop].items()
            }
        if "stage" in kwargs:
            kwargs["stage"] = Stage[kwargs["stage"].upper()]
        return cls(**kwargs)


class Output(NamedTuple):
    name: str
    select: List[Definition]
    fill: Any
    single: bool = False
    ismap: bool = False


class Input(object):
    """
    All inputs have a unique id. Without it they can't be tracked for locking
    purposes.
    """

    def __init__(
        self,
        value: Any,
        definition: Definition,
        parents: Optional[List["Input"]] = None,
        *,
        uid: Optional[str] = "",
    ):
        if parents is None:
            parents = []
        self.value = value
        self.definition = definition
        self.parents = parents
        self.uid = uid
        if not self.uid:
            self.uid = str(uuid.uuid4())

    def get_parents(self) -> Iterator["Input"]:
        return list(
            set(
                itertools.chain(
                    *[
                        [item] + list(set(item.get_parents()))
                        for item in self.parents
                    ]
                )
            )
        )

    def __repr__(self):
        return "%s: %s" % (self.definition.name, self.value)

    def __str__(self):
        return repr(self)

    def export(self):
        return dict(value=self.value, definition=self.definition.export())

    @classmethod
    def _fromdict(cls, **kwargs):
        kwargs["definition"] = Definition._fromdict(**kwargs["definition"])
        return cls(**kwargs)


class Parameter(NamedTuple):
    key: str
    value: Any
    origin: Input
    definition: Definition


class InputFlow(dict):
    """
    Inputs of an operation by their name as used by the operation implementation
    mapped to a list of locations they can come from. The list contains strings
    in the format of operation_instance_name.key_in_output_mapping or the
    literal "seed" which specifies that the value could be seeded to the
    network.
    """

    def export(self):
        return dict(self)


@dataclass
class DataFlow:
    operations: Dict[str, Union[Operation, Callable]]
    seed: List[Input] = field(default=None)
    configs: Dict[str, BaseConfig] = field(default=None)
    definitions: Dict[str, Definition] = field(init=False)
    flow: Dict[str, InputFlow] = field(default=None)
    # Implementations can be provided in case they haven't been registered via
    # the entrypoint system.
    implementations: Dict[str, "OperationImplementation"] = field(default=None)

    def __post_init__(self):
        # Prevent usage of a global dict (if we set default to {} then all the
        # instances will share the same instance of that dict, or list)
        if self.seed is None:
            self.seed = []
        if self.configs is None:
            self.configs = {}
        if self.flow is None:
            self.flow = {}
        if self.implementations is None:
            self.implementations = {}
        # Allow callers to pass in functions decorated with op. Iterate over the
        # given operations and replace any which have been decorated with their
        # operation. Add the implementation to our dict of implementations.
        for instance_name, value in self.operations.items():
            if (
                getattr(value, "imp", None) is not None
                and getattr(value, "op", None) is not None
            ):
                # Get the operation and implementation from the wrapped object
                operation = getattr(value, "op", None)
                opimp = getattr(value, "imp", None)
                # Set the implementation if not explicitly set
                self.implementations.setdefault(operation.name, opimp)
                # Change this entry to the instance of Operation associated with
                # the wrapped object
                self.operations[instance_name] = operation
                value = operation
            # Make sure every operation has the correct instance name
            self.operations[instance_name] = value._replace(
                instance_name=instance_name
            )
        # Grab all definitions from operations
        operations = list(self.operations.values())
        definitions = list(
            set(
                itertools.chain(
                    *[
                        itertools.chain(
                            operation.inputs.values(),
                            operation.outputs.values(),
                        )
                        for operation in operations
                    ]
                )
            )
        )
        definitions = {
            definition.name: definition for definition in definitions
        }
        self.definitions = definitions

    def export(self, *, linked: bool = False):
        exported = {
            "operations": {
                instance_name: operation.export()
                for instance_name, operation in self.operations.items()
            }
            if not linked
            else self._linked_operations(),
            "seed": self.seed.copy(),
            "configs": self.configs.copy(),
            "flow": self.flow.copy(),
        }
        if linked:
            exported["linked"] = True
            exported["definitions"] = self.definitions.copy()
        return export_dict(**exported)

    @classmethod
    def _fromdict(cls, *, linked: bool = False, **kwargs):
        # Import all operations
        if linked:
            kwargs["operations"] = cls._resolve_operations(kwargs)
            del kwargs["definitions"]
        kwargs["operations"] = {
            instance_name: Operation._fromdict(
                instance_name=instance_name, **operation
            )
            for instance_name, operation in kwargs["operations"].items()
        }
        # Import seed inputs
        kwargs["seed"] = [
            Input._fromdict(**input_data) for input_data in kwargs["seed"]
        ]
        # Import input flows
        kwargs["flow"] = {
            instance_name: InputFlow(input_flow)
            for instance_name, input_flow in kwargs["flow"].items()
        }
        return cls(**kwargs)

    @classmethod
    def auto(cls, *operations):
        flow_dict = {}
        # Create output_dict, which maps all of the definitions to the
        # operations that create them.
        output_dict = {}
        for operation in operations:
            for output in operation.outputs.values():
                output_dict.setdefault(output.name, {})
                output_dict[output.name].update({operation.name: operation})
        # Got through all the operations and look at their inputs
        for operation in operations:
            flow_dict.setdefault(operation.name, InputFlow())
            # Example operation:
            # Operation(
            #     name="pypi_package_json",
            #     # internal_name: package
            #     # definition: package = Definition(name="package", primitive="str")
            #     inputs={"package": package},
            #     # internal_name: response_json
            #     # definition: package_json = Definition(name="package_json", primitive="Dict")
            #     outputs={"response_json": package_json},
            # )
            # For each input
            for internal_name, definition in operation.inputs.items():
                # With pypi_package_json example
                # internal_name = "package"
                # definition = package
                #            = Definition(name="package", primitive="str")
                if definition.name in output_dict:
                    # Grab the dict of operations that produce this definition
                    # as an output
                    producing_operations = output_dict[definition.name]
                    # If the input could be produced by an operation in the
                    # network, then it's definition name will be in output_dict.
                    flow_dict[operation.name][internal_name] = []
                    # We look through the outputs and add any one that matches
                    # the definition and add it to the list in format of
                    # operation_name . internal_name (of output)
                    for producting_operation in producing_operations.values():
                        for (
                            internal_name_of_output,
                            output_definition,
                        ) in producting_operation.outputs.items():
                            if output_definition == definition:
                                flow_dict[operation.name][
                                    internal_name
                                ].append(
                                    producting_operation.name
                                    + "."
                                    + internal_name_of_output
                                )
                else:
                    flow_dict[operation.name][internal_name] = ["seed"]
        return cls(
            operations={operation.name: operation for operation in operations},
            flow=flow_dict,
        )

    @classmethod
    def _resolve_operations(cls, source: Dict):
        definitions = {}
        operations = {}
        for name, definition in source.get("definitions", {}).items():
            definition.setdefault("name", name)
            definitions[name] = definition
        for instance_name, operation in source.get("operations", {}).items():
            # Replaces strings referencing definitions with definitions
            for arg in ["conditions"]:
                if not arg in operation:
                    continue
                for i, definition_name in enumerate(operation[arg]):
                    if not definition_name in definitions:
                        raise DefinitionMissing(
                            "While resolving {instance_name}.{arg}, missing {definition_name}"
                        )
                    operation[arg][i] = definitions[definition_name]
            for arg in ["inputs", "outputs"]:
                if not arg in operation:
                    continue
                for input_name, definition_name in operation[arg].items():
                    if not definition_name in definitions:
                        raise DefinitionMissing(
                            "While resolving {instance_name}.{arg}, missing {definition_name}"
                        )
                    operation[arg][input_name] = definitions[definition_name]
            operation.setdefault("name", name)
        return source["operations"]

    def _linked_operations(self):
        exported = {}
        for operation in self.operations.values():
            exported_operation = operation.export()
            for name, definition in operation.inputs.items():
                exported_operation["inputs"][name] = definition.name
            for name, definition in operation.outputs.items():
                exported_operation["outputs"][name] = definition.name
            exported[operation.instance_name] = exported_operation
        return exported
