class InvalidEnvelopeError(Exception):
    """Input envelope is not callable and instance of BaseEnvelope"""


class InvalidModelTypeError(Exception):
    """Input data model does not implement BaseModel"""


class InvalidEnvelopeChaining(Exception):
    """Input Envelopes combination does not support chaining"""
