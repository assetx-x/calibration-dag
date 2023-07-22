from commonlib.security_master_utils.isin import ISIN
import string
from functools import reduce

def convert_cusip_to_isin(cusip_code):
    """
    Calculates the ISIN digit number from the CUSIP number; the 'US' contry code has been added.
    ISIN = "US" + CUSIP + check digit

    Args
    ----
       v = str
          CUSIP number for the security

    Returns
    -------
       ISIN for the given CUSIP
    """

    numerical_codes = map(lambda x: ord(x) - ord("A") + 10 if not x.isdigit() else int(x),
                          list("US{0}".format(cusip_code[0:9].zfill(9))))
    all_digits = "".join(reduce(lambda x,y:x+y, map(lambda x:list(str(x)),numerical_codes),[]))
    even_digits = map(int,list(all_digits[1::2]))
    odd_digits = map(int,list(all_digits[0::2]))
    if len(all_digits) % 2 == 0:
        even_digits = map(lambda x:x*2, even_digits)
    else:
        odd_digits = map(lambda x:x*2, odd_digits)
    full_expanded_digits = reduce(lambda a,b:a+b, map(lambda x:list(str(x)), even_digits), []) +\
        reduce(lambda a,b:a+b, map(lambda x:list(str(x)), odd_digits), [])
    digits_sum = sum(map(int, full_expanded_digits))
    check_digit = (10 - digits_sum % 10) % 10
    isin_number = "US{0}{1}".format(cusip_code[0:9].zfill(9), check_digit)
    if not verify_isin(isin_number):
        print("Check digit '%d' not valid for for CUSIP '%s'" %(check_digit, cusip_code))
    #return "US{0}{1}".format(cusip_code[0:9].zfill(9), check_digit)
    return isin_number

def convert_isin_to_cusip(isin_code):
    """
    A CUSIP is a 9-character alphanumeric code which identifies a North American financial security.
    - A six-character issuer code
    - A two-character issue number
    - A single check digit.
    Calculates the correct ISIN from CUSIP.

    Args
    ----
       isin_code: str
          ISIN value for the security

    Raises
    ------
       CheckdigitError if the isin digit is invalid.

    Returns
    -------
       CUSIP for the security
    """
    if verify_isin(isin_code):
        letters_ls = list(string.ascii_uppercase)
        isin2 = []
        if isin_code[:2].isalpha():
            cusip = isin_code[2:-1]
        for c in range(len(isin_code[:-1])):
            if isin_code[c].isalpha():
                val = letters_ls.index(isin_code[c]) + 10
                isin2.append(val)
            else:
                isin2.append(isin_code[c])
        isin2 = ''.join([str(i) for i in isin2])
        even = isin2[::2]
        odd = isin2[1::2]
        if len(isin2) % 2 > 0:
            even = ''.join([str(int(i)*2) for i in list(even)])
        else:
            odd = ''.join([str(int(i)*2) for i in list(odd)])
        even_sum = sum([int(i) for i in even])
        odd_sum = sum([int(i) for i in odd])
        mod = (even_sum + odd_sum) % 10
        digit = 10 - mod
        if digit == int(isin_code[-1]):
            print("CUSIP number '%s' for '%s'" %(cusip, isin_code))
            return cusip
        else:
            raise ReferenceError("Checkdigit '%s' is not valid for '%s'" % (digit, isin_code))

def verify_isin(isin_code):
    v = ISIN(isin_code)
    return v.__str__() == isin_code

