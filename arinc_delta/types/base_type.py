# -*- coding: utf-8 -*-
from typing import Dict, Any, List, Optional, Tuple

# Tip modülleri bu arayüzü uygular:
# TYPE_CODE: str  -> "PG", "PI", "PV"
# SELECTED_TYPE: Tuple[str,str] -> ("P","G") gibi (Section/Subsection)
# COLS: List[(name, (start,end)) or (name, None)]  -> None => primary_1_123 ham payload
# postprocess_row(row: Dict[str,Any]) -> None      -> tip-özel düzeltmeler (örn. ident 4’e sabitle)
