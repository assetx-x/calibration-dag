import logging
import hashlib
import re
from uuid import uuid4

import requests
from lxml import etree

_logger = logging.getLogger('luigi-interface')

# _session_id = "08:00:27:AA:68:5B"
_session_id = "50:9A:4C:4E:0C:DF"

_capiq_url = "https://capitaliq.com/CIQDotNet"
_capiq_fq_url = "https://www.capitaliq.com/CIQDotNet"
_capiq_version = "9.10.211.6378"

_common_headers = {
    'Accept': None,
    'Accept-Encoding': None,
    "Content-Type": "text/xml; charset=utf-8",
    "Host": "capitaliq.com",
    "Connection": "close"
}


def _capiq_request_url(request_type):
    return _capiq_url + "/Excel/{request_type}/{capiq_version}.axd".format(
        request_type=request_type, capiq_version=_capiq_version)


def _enrich_headers(request_headers):
    return dict(_common_headers, **request_headers)


def _flatten_xml(xml_fragment):
    flattened = re.sub(' +', ' ', xml_fragment.replace('\n', ''))
    flattened = re.sub('> <', '><', flattened)
    return flattened.strip()


class _CapIQLoginHelper(object):
    _login_url = _capiq_request_url(request_type="106")

    _login_headers = {
        "User-Agent": "CIQ Excel Plug-In",
        "Accept": "text/protobuff"
    }

    _login_request = """
        <Envelope>
            <Header>
                <Item name="Username" value="{username}"/>
                <Item name="Password" value="{password}"/>
                <Item name="ReleaseID" value="9100"/>
                <Item name="Compression" value="False"/>
                <Item name="SessionID" value="{session_id}"/>
                <Item name="RequestType" value="106"/>
                <Item name="DateFormatID" value="0"/>
                <Item name="DecimalSeparator" value="."/>
                <Item name="LocaleID" value="1033"/>
                <Item name="ListSeparator" value=","/>
                <Item name="ExcelVersion" value="16"/>
                <Item name="expenseCode" value=""/>
                <Item name="OSVer" value="10.0.15063.0"/>
                <Item name="OSBits" value="X64"/>
            </Header>
            <Body>
                <Param id="1">
                    <Request>
                        <Major>9</Major>
                        <Minor>10</Minor>
                        <Build>211</Build>
                        <Revision>6378</Revision>
                    </Request>
                </Param>
            </Body>
        </Envelope>"""

    def __init__(self):
        self.encrypted_password = None
        self.cookies = None

    def login(self, username, password):
        login_request = self._login_request.format(username=username, password=password, session_id=_session_id)
        login_request = _flatten_xml(login_request)
        headers = _enrich_headers(self._login_headers)
        response = requests.post(self._login_url, login_request, headers=headers)
        if not response.ok:
            raise RuntimeError("CapIQ login request failed: %d %s" % (response.status_code, response.reason))
        self._parse_response(response)
        self.cookies = response.cookies
        return True

    def _parse_response(self, response):
        parser = etree.XMLParser()
        xml_tree = etree.fromstring(response.content, parser)
        password_elem = xml_tree.xpath("/Envelope/Response/EncryptedPW")
        if len(password_elem) == 1:
            self.encrypted_password = password_elem[0].text
            self.cookies = response.cookies
            self.is_logged_in = True
            _logger.info("CapIQ successfully logged in")
        else:
            self._parse_error_message(xml_tree, response)

    @staticmethod
    def _parse_error_message(xml_tree, response):
        error = xml_tree.xpath("/Envelope/Response/Error")
        if len(error) == 1:
            raise RuntimeError("CapIQ login failed: [%s] %s" % (error[0].attrib["code"], error[0].text))
        else:
            raise RuntimeError("CapIQ login failed, unknown error: %s" % response.content)


class _CapIQLogoutHelper(object):
    _logout_url = _capiq_url + "/{capiq_version}/Authenticator.asmx".format(capiq_version=_capiq_version)

    _logout_headers = {
        "User-Agent": "CIQ Office Plug-In",
        "SOAPAction": _capiq_fq_url + "/Authenticator/Logout"
    }

    _logout_request = """
        <?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                       xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <soap:Body>
                <Logout xmlns="{capiq_url}/Authenticator" />
            </soap:Body>
        </soap:Envelope>""".format(capiq_url=_capiq_fq_url)

    def logout(self, cookies):
        request = _flatten_xml(self._logout_request)
        headers = _enrich_headers(self._logout_headers)
        response = requests.post(self._logout_url, request, headers=headers, cookies=cookies)
        if response.ok:
            _logger.info("CapIQ successfully logged out")
        else:
            _logger.warning("CapIQ logout request failed: %d %s" % (response.status_code, response.reason))


class _CapIQAuthenticator(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.encrypted_password = None
        self.cookies = None
        self.is_logged_in = False

    def login(self):
        if not self.is_logged_in:
            helper = _CapIQLoginHelper()
            if helper.login(self.username, self.password):
                self.encrypted_password = helper.encrypted_password
                self.cookies = helper.cookies
                self.is_logged_in = True

    def logout(self):
        if self.is_logged_in:
            helper = _CapIQLogoutHelper()
            helper.logout(self.cookies)
            self.encrypted_password = None
            self.cookies = None
            self.is_logged_in = False


class CapIQClient(object):
    _request_url = _capiq_request_url(request_type="103")

    _request_template = """
    <Envelope>
        <Header>
            <Item name="Username" value="{username}"/>
            <Item name="Password" value="{encrypted_password}"/>
            <Item name="ReleaseID" value="9100"/>
            <Item name="Compression" value="{use_compression}"/>
            <Item name="SessionID" value="{session_id}"/>
            <Item name="RequestType" value="103"/>
            <Item name="DateFormatID" value="0"/>
            <Item name="DateFormat" value="yyyy-MM-dd"/>
            <Item name="DecimalSeparator" value="."/>
            <Item name="LocaleID" value="1033"/>
            <Item name="ListSeparator" value=","/>
            <Item name="ExcelVersion" value="16"/>
            <Item name="expenseCode" value=""/>
            <Item name="ConversionMode" value="H"/>
            <Item name="CurrencyType" value="LOCAL"/>
            <Item name="FilingType" value="P"/>
            <Item name="RestatedFlag" value="L"/>
            <Item name="EstimatesDataVendorId" value="-1"/>
            <Item name="UploadCurrency" value="REPORTED"/>
            <Item name="UploadUnit" value="MM"/>
            <Item name="AttachCreate" value="A"/>
            <Item name="PropDataSource" value="LA"/>
            <Item name="TimeZoneUTCOffset" value="-05:00:00" />
        </Header>
        <Body>
            <Param id="1">
                <Request TransactionId = "{transaction_id}">{items}</Request>
            </Param>
        </Body>
    </Envelope>"""

    _content_type_xml = "text/xml; charset=utf-8"
    _content_type_gzip = "application/x-gzip; charset=utf-8"

    _request_headers = {
        "User-Agent": "CIQ Excel Plug-In",
        "Content-Type": _content_type_xml
    }

    def __init__(self, username, password, retries=3, use_compression=True):
        self.retries = retries
        self.use_compression = use_compression
        self._request_headers = _enrich_headers(self._request_headers)
        if use_compression:
            self._request_headers["Content-Type"] = self._content_type_gzip
        self._authenticator = _CapIQAuthenticator(username, password)

    def login(self):
        self._authenticator.login()
        self._request_template = self._request_template.format(
            username=self._authenticator.username,
            encrypted_password=self._authenticator.encrypted_password,
            use_compression=self.use_compression,
            session_id=_session_id,
            transaction_id="{transaction_id}",
            items="{items}")

    def logout(self):
        self._authenticator.logout()

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()

    def request(self, request_items, fields=None):
        if not self._authenticator.is_logged_in:
            raise RuntimeError("CapIQ client is not logged in. Must login before sending requests")

        rendered_request = self._render_request(request_items)
        items = self._retrieve_response_items(rendered_request)

        if not isinstance(request_items, dict):
            response = []
            items = sorted(items, key=lambda itm: int(itm.attrib["cellAddress"]))
        else:
            response = {}

        for item in items:
            if item.xpath("Function"):
                assert fields
                # Needs additional request to collect dicts
                bits = self.request([
                    "=CIQ(%s, %s)" % (item_func.text, field)
                    for item_func in item.xpath("Function")
                    for field in fields
                ])
                dicts = []
                while bits:
                    next_dict = {}
                    for field in fields:
                        next_dict[field] = bits.pop()
                    dicts.append(next_dict)
                if not isinstance(request_items, dict):
                    response.append(dicts)
                else:
                    response[item.attrib["cellAddress"]] = dicts
            else:
                # noinspection PyBroadException
                try:
                    value = item.xpath("value")[-1].text
                except Exception:
                    value = None

                if not isinstance(request_items, dict):
                    response.append(value)
                else:
                    response[item.attrib["cellAddress"]] = value

        return response

    def _render_request(self, request_items):
        if not isinstance(request_items, dict):
            rendered_items = [self._render_request_item(item, i) for i, item in enumerate(request_items)]
        else:
            rendered_items = [self._render_request_item(item, key) for key, item in request_items.items()]
        return self.render_request_body(rendered_items)

    def render_request_body(self, rendered_items):
        rendered_request = self._request_template.format(
            transaction_id=hashlib.md5(str(uuid4()).encode()).hexdigest(),
            items="".join(rendered_items)
        )
        rendered_request = _flatten_xml(rendered_request)

        if self.use_compression:
            with _ContentCoder(self._content_type_gzip, rendered_request) as coder:
                rendered_request = coder.encode()
        return rendered_request

    @staticmethod
    def _render_request_item(data, identifier):
        match = re.match(r"^=(?P<name>[A-Z0-9_]+)\((?P<arguments>[^)]+)\)", data)
        if match is None:
            raise ValueError("Illegal query item: %s" % data)

        arguments = [x.strip() for x in match.group("arguments").split(",")]
        arguments = [x[1:] if x[0] in ["'", "\""] else x for x in arguments]
        arguments = [x[:-1] if x[-1] in ["'", "\""] else x for x in arguments]
        rendered_arguments = " ".join(["parm%d=\"%s\"" % (i + 1, x) for i, x in enumerate(arguments)])

        return """<{name} {rendered_arguments} id="1" Layout="0" cellAddress="{identifier}"/>""".format(
            name=match.group("name"),
            rendered_arguments=rendered_arguments,
            identifier=identifier
        )

    def _retrieve_response_items(self, rendered_request):
        attempts = 3
        while attempts > 0:
            try:
                http_response = self.post_request(rendered_request)
                content = self.decode_response(http_response)
                items = self.parse_content(content, http_response)
                return items
            except:
                attempts -= 1
                if attempts == 0:
                    raise

    def post_request(self, request):
        cookies = self._authenticator.cookies
        retries = self.retries
        while retries > 0:
            response = requests.post(self._request_url, request, headers=self._request_headers, cookies=cookies)
            if response.ok:
                return response
            retries -= 1
            if retries == 0:
                raise RuntimeError("CapIQ request failed: [%d] %s" % (response.status_code, response.reason))

    def decode_response(self, response):
        if "content-type" in response.headers:
            content_type = response.headers["content-type"]
        else:
            content_type = self._content_type_gzip if self.use_compression else self._content_type_xml
            _logger.warning("Received CapIQ response without a content type. Guessing content type to be '%s'",
                            content_type)
            _logger.warning("Headers: %s", list(response.headers.keys()))
            _logger.warning("Content[:160]: %s", response.content[:160])

        with _ContentCoder(content_type, response.content) as coder:
            content = coder.decode()
            return content

    @staticmethod
    def parse_content(content, response):
        parser = etree.XMLParser()
        try:
            xml_tree = etree.fromstring(content, parser)
            return list(xml_tree.xpath("/Response")[0])
        except:
            _logger.error("Failed to parse CapIQ response: [%d] %s", response.status_code, response.reason)
            _logger.error("Headers: %s", list(response.headers.keys()))
            _logger.error("Content: %s", response.content)
            raise


class _ContentCoder(object):
    def __init__(self, content_type, content):
        self.content_type = content_type
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def encode(self):
        if self.content_type.startswith("application/x-gzip"):
            return self._encode_gzip()
        else:
            return self.content

    def _encode_gzip(self):
        from gzip import GzipFile
        from io import BytesIO
        out = BytesIO()
        with GzipFile(fileobj=out, mode="w") as gzip_file:
            gzip_file.write(self.content.encode())
        return out.getvalue()

    def decode(self):
        if self.content_type.startswith("application/x-gzip"):
            return self._decode_gzip()
        elif self.content_type.startswith("application/x-bzip2"):
            return self._decode_bzip2()
        else:
            return self.content

    def _decode_gzip(self):
        from gzip import GzipFile
        from io import BytesIO
        content_buf = BytesIO(self.content)
        with GzipFile(mode='rb', fileobj=content_buf) as gzip_file:
            return gzip_file.read()

    def _decode_bzip2(self):
        from bz2 import BZ2Decompressor
        decompressor = BZ2Decompressor()
        return decompressor.decompress(self.content)


# noinspection SpellCheckingInspection
def _main():
    import sys
    from pprint import pprint

    log_format = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s"
    log_formatter = logging.Formatter(log_format, "%Y-%m-%d %H:%M:%S")
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(log_formatter)
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().handlers = [console_handler]

    def _apply_template_nd(template, ticker, metric):
        key = "{ticker};{metric}".format(ticker=ticker, metric=metric)
        formula = template.format(ticker=ticker, metric=metric)
        return key, formula

    def _apply_template(suffix, template, ticker, metric, date):
        from datetime import datetime
        date = date if date else datetime.now().strftime("%Y-%m-%d")
        key = "{ticker};{date};{metric}".format(ticker=ticker, metric=metric, date=date)
        key = key + "_" + suffix if suffix else key
        formula = template.format(ticker=ticker, metric=metric, date=date)
        return key, formula

    def _ciq_nd(ticker, metric):
        return _apply_template_nd("""=CIQ("{ticker}", "{metric}")""", ticker, metric)

    def _ciq(ticker, metric, date=None):
        return _apply_template("", """=CIQ("{ticker}", "{metric}", "{date}", "", "", "", "USD")""",
                               ticker, metric, date)

    def _ciq_cq(ticker, metric, date=None):
        return _apply_template("", """=CIQ("{ticker}", "{metric}", "5000", "{date}", "", "", "USD")""",
                               ticker, metric, date)

    def _ciq_ltm(ticker, metric, date=None):
        return _apply_template("", """=CIQ("{ticker}", "{metric}", "2000", "{date}", "", "", "USD")""",
                               ticker, metric, date)

    def _ciq_ntm(ticker, metric, date=None):
        return _apply_template("", """=CIQ("{ticker}", "{metric}", "6000", "{date}", "", "", "USD"))""",
                               ticker, metric, date)

    with CapIQClient("hhuyette@aceandcompany.com", "5GEhb(1rUW^$") as client:
        pprint(client.request(dict([
            _ciq_nd("ABMD", "IQ_COMPANY_ID"),
            _ciq_nd("ABMD", "IQ_COMPANY_NAME"),
            _ciq_nd("ABMD", "IQ_EXCHANGE"),
            _ciq_nd("ABMD", "IQ_INDUSTRY"),
            _ciq_nd("ABMD", "IQ_INDUSTRY_SECTOR"),
            _ciq_nd("ABMD", "IQ_ISIN"),
            _ciq_nd("ABMD", "IQ_TRADING_CURRENCY"),
            _ciq_nd("ABMD", "IQ_COMPANY_TYPE"),
            _ciq_nd("ABMD", "IQ_SECURITY_NAME"),
            _ciq_nd("ABMD", "IQ_SECURITY_TYPE"),
            _ciq_nd("ABMD", "IQ_COUNTRY_NAME"),
            _ciq_nd("ABMD", "IQ_TRADING_ITEM_CIQID"),
            _ciq_nd("ABMD", "IQ_SECURITY_ITEM_CIQID"),
        ])))
        # rr = client.request({
        #     "BF.A:EQUITY_LIST": '=CIQRANGE("BF.A", "IQ_EQUITY_LIST", 1, 10, "", "", "", "", "Equity Identifiers")',
        #     "BF.B:EQUITY_LIST": '=CIQRANGE("BF.A", "IQ_EQUITY_LIST", 1, 10, "", "", "", "", "Equity Identifiers")',
        #     "BRK.B:EQUITY_LIST": '=CIQRANGE("BRK.B", "IQ_EQUITY_LIST", 1, 10, "", "", "", "", "Equity Identifiers")',
        # })
        print("breakpoint")
    print(">>>>> done")


if __name__ == '__main__':
    _main()
