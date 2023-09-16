import itertools
import logging
from typing import Any, Callable, Dict, List, Optional, Type, Union, overload

from aws_lambda_powertools.utilities.parser.compat import disable_pydantic_v2_warning
from aws_lambda_powertools.utilities.parser.models.raw_event import RawEvent
from aws_lambda_powertools.utilities.parser.types import EventParserReturnType, Model

from ...middleware_factory import lambda_handler_decorator
from ..typing import LambdaContext
from .envelopes.base import Envelope
from .exceptions import InvalidEnvelopeChaining, InvalidEnvelopeError, InvalidModelTypeError

logger = logging.getLogger(__name__)


@lambda_handler_decorator
def event_parser(
    handler: Callable[[Any, LambdaContext], EventParserReturnType],
    event: Dict[str, Any],
    context: LambdaContext,
    model: Type[Model],
    envelope: Optional[Union[Type[Envelope], List[Type[Envelope]]]] = None,
) -> EventParserReturnType:
    """Lambda handler decorator to parse & validate events using Pydantic models

    It requires a model that implements Pydantic BaseModel to parse & validate the event.

    When an envelope is given, it'll use the following logic:

    1. Parse the event against the envelope model first e.g. EnvelopeModel(**event)
    2. Envelope will extract a given key to be parsed against the model e.g. event.detail

    This is useful when you need to confirm event wrapper structure, and
    b) selectively extract a portion of your payload for parsing & validation.

    NOTE: If envelope is omitted, the complete event is parsed to match the model parameter BaseModel definition.

    Example
    -------
    **Lambda handler decorator to parse & validate event**

        class Order(BaseModel):
            id: int
            description: str
            ...

        @event_parser(model=Order)
        def handler(event: Order, context: LambdaContext):
            ...

    **Lambda handler decorator to parse & validate event - using built-in envelope**

        class Order(BaseModel):
            id: int
            description: str
            ...

        @event_parser(model=Order, envelope=envelopes.EVENTBRIDGE)
        def handler(event: Order, context: LambdaContext):
            ...

    Parameters
    ----------
    handler:  Callable
        Method to annotate on
    event:    Dict
        Lambda event to be parsed & validated
    context:  LambdaContext
        Lambda context object
    model:   Model
        Your data model that will replace the event.
    envelope: Envelope
        Optional envelope to extract the model from

    Raises
    ------
    ValidationError
        When input event does not conform with model provided
    InvalidModelTypeError
        When model given does not implement BaseModel
    InvalidEnvelopeError
        When envelope given does not implement BaseEnvelope
    """
    parsed_event: Union[Model, List[Any], Any] = None
    if not envelope:
        parsed_event = parse(event=event, model=model)
    elif isinstance(envelope, List):
        parsed_event = chained_parse(event=event, model=model, envelopes=envelope)
    else:
        parsed_event = parse(event=event, model=model, envelope=envelope)

    logger.debug(f"Calling handler {handler.__name__}")
    return handler(parsed_event, context)


@overload
def parse(event: Dict[str, Any], model: Type[Model]) -> Model:
    ...  # pragma: no cover


@overload
def parse(event: Dict[str, Any], model: Type[Model], envelope: Type[Envelope]):
    ...  # pragma: no cover


def parse(event: Dict[str, Any], model: Type[Model], envelope: Optional[Type[Envelope]] = None):
    """Standalone function to parse & validate events using Pydantic models

    Typically used when you need fine-grained control over error handling compared to event_parser decorator.

    Example
    -------

    **Lambda handler decorator to parse & validate event**

        from aws_lambda_powertools.utilities.parser import ValidationError

        class Order(BaseModel):
            id: int
            description: str
            ...

        def handler(event: Order, context: LambdaContext):
            try:
                parse(model=Order)
            except ValidationError:
                ...

    **Lambda handler decorator to parse & validate event - using built-in envelope**

        class Order(BaseModel):
            id: int
            description: str
            ...

        def handler(event: Order, context: LambdaContext):
            try:
                parse(model=Order, envelope=envelopes.EVENTBRIDGE)
            except ValidationError:
                ...

    Parameters
    ----------
    event:    Dict
        Lambda event to be parsed & validated
    model:   Model
        Your data model that will replace the event
    envelope: Envelope
        Optional envelope to extract the model from

    Raises
    ------
    ValidationError
        When input event does not conform with model provided
    InvalidModelTypeError
        When model given does not implement BaseModel
    InvalidEnvelopeError
        When envelope given does not implement BaseEnvelope
    """
    if envelope and callable(envelope):
        try:
            logger.debug(f"Parsing and validating event model with envelope={envelope}")
            return envelope().parse(data=event, model=model)
        except AttributeError:
            raise InvalidEnvelopeError(f"Envelope must implement BaseEnvelope, envelope={envelope}")

    try:
        disable_pydantic_v2_warning()
        logger.debug("Parsing and validating event model; no envelope used")
        if isinstance(event, str):
            return model.parse_raw(event)

        return model.parse_obj(event)
    except AttributeError:
        raise InvalidModelTypeError(f"Input model must implement BaseModel, model={model}")


def _chained_parse(events: List[Dict[str, Any]], model: Type[Model], envelopes: List[Type[Envelope]]) -> List:
    print(type(envelopes))
    if len(envelopes) == 1:
        envelope = envelopes[0]
        print(f"{events=}, {model=}, {envelope=}")
        res = [parse(event=event, model=model, envelope=envelope) for event in events]
        if isinstance(res[0], List):
            return list(itertools.chain.from_iterable(res))
        return res

    envelope = envelopes[0]
    dict_events = []
    for event in events:
        parsed_event: Union[RawEvent, List[RawEvent]] = parse(event=event, model=RawEvent, envelope=envelope)
        if isinstance(parsed_event, RawEvent):
            dict_events.append(parsed_event.as_raw_dict())
        elif isinstance(parsed_event, List) and isinstance(parsed_event[0], RawEvent):
            dict_events.extend(x.as_raw_dict() for x in parsed_event)
        else:
            raise InvalidEnvelopeChaining(
                f"Return type expected is {RawEvent} or {List[RawEvent]}, "
                f"received {type(parsed_event)} from envelope {envelope}",
            )

    return list(itertools.chain.from_iterable(_chained_parse(events=dict_events, model=model, envelopes=envelopes[1:])))


def chained_parse(event: Dict[str, Any], model: Type[Model], envelopes: List[Type[Envelope]]) -> List:
    return _chained_parse(events=[event], model=model, envelopes=envelopes)
