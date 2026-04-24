"""Regression tests: parsers must not resolve external XML entities (XXE).

We inject a classic `file:///etc/passwd` entity and assert:
- no content from the local filesystem leaks into the parsed output,
- parsers fail gracefully (empty DataFrame or None root) rather than crashing.
"""

from __future__ import annotations

from piboufilings.parsers.form_13f_parser import Form13FParser
from piboufilings.parsers.form_nport_parser import _safe_fromstring
from piboufilings.parsers.form_sec16_parser import FormSection16Parser

_XXE_13F = """<?xml version="1.0"?>
<!DOCTYPE data [
  <!ENTITY xxe SYSTEM "file:///etc/passwd">
]>
<informationTable>
  <infoTable>
    <nameOfIssuer>&xxe;</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>000000000</cusip>
    <value>100</value>
    <shrsOrPrnAmt><sshPrnamt>1</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>
    <votingAuthority><Sole>1</Sole><Shared>0</Shared><None>0</None></votingAuthority>
  </infoTable>
</informationTable>
"""

_XXE_SEC16 = """<?xml version="1.0"?>
<!DOCTYPE data [
  <!ENTITY xxe SYSTEM "file:///etc/passwd">
]>
<ownershipDocument>
  <schemaVersion>&xxe;</schemaVersion>
  <documentType>4</documentType>
</ownershipDocument>
"""


def test_13f_parser_rejects_external_entities(tmp_path):
    """defusedxml raises EntitiesForbidden, which the parser swallows → empty holdings."""
    parser = Form13FParser(output_dir=tmp_path)
    df = parser._parse_holdings(_XXE_13F, "028-1", "20240101")
    # Either empty DF (defusedxml blocked the DOCTYPE), or if anything slipped
    # through, the issuer name must NOT contain /etc/passwd content.
    if not df.empty:
        assert "root:" not in str(df.iloc[0].get("NAME_OF_ISSUER", "")), (
            "XXE RESOLVED — the /etc/passwd content leaked into 13F output"
        )


def test_sec16_parser_rejects_external_entities(tmp_path):
    parser = FormSection16Parser(output_dir=tmp_path)
    root = parser._get_xml_root(_XXE_SEC16)
    if root is not None:
        sv = root.findtext("schemaVersion") or ""
        assert "root:" not in sv, "XXE RESOLVED in Section 16 parser"


def test_lxml_safe_parser_does_not_resolve_entities():
    # lxml with resolve_entities=False returns the document but substitutes an
    # empty string for the unresolved entity reference.
    root = _safe_fromstring(_XXE_13F.encode())
    text = root.findtext(".//nameOfIssuer") or ""
    assert "root:" not in text, "XXE RESOLVED by NPORT lxml parser"
