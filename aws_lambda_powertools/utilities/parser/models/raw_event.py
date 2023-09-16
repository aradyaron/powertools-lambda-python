from typing import Any, Dict

from aws_lambda_powertools.utilities.parser.compat import pydantic_version

if pydantic_version() == 1:
    from pydantic import BaseModel

    class RawEvent(BaseModel):
        __root__: Dict[str, Any]

        def as_raw_dict(self) -> Dict[str, Any]:
            return self.__root__

else:
    from pydantic import RootModel  # type: ignore[attr-defined]

    class RawEvent(RootModel):  # type: ignore[no-redef]
        root: Dict[str, Any]

        def as_raw_dict(self) -> Dict[str, Any]:
            return self.root
