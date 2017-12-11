
/*
 Licensed Materials - Property of IBM
 
 License: BSD 3-Clause

 5747-C31, 5747-C32

 © Copyright IBM Corp. 2017    All Rights Reserved

 US Government Users Restricted Rights - Use, duplication or disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

*/
/*
 * rwedb2_utils.cpp
 *
 *  Created on: Mar 17, 2017
 *      Author: karthik
 */

#include "rwedb2_utils.h"
#include "utf8.h"

#ifdef RDB2_DEBUG
#include <iostream>
#include <iomanip>
#endif

namespace rdb2 {

std::string extract_error(const std::string& fn, SQLHANDLE handle, SQLSMALLINT type) {
  SQLINTEGER i = 0;
  SQLINTEGER native = 0;
  SQLCHAR state[7] = "";
  SQLCHAR text[256] = "";
  SQLSMALLINT len = 0;
  SQLRETURN ret = 0;

  std::string msg = "";

  msg.append("The driver reported the following diagnostics whilst running ");
  msg.append("\n");
  msg.append(fn);
  msg.append(" ");

  do {
    ret = SQLGetDiagRec(type, handle, ++i, state, &native, text, sizeof(text), &len);
    if (SQL_SUCCEEDED(ret)) {
      msg.append((const char*) state);
      msg += ":";
      msg += native;
      msg += ":";
      msg += (const char*) text;
      msg += " \n";

    }
  } while (ret == SQL_SUCCESS);

  return msg;
}

void encodeUTF8StringAsUTF16(SQLWCHAR* const dest, const std::string& source) {
/*
 * encodes string in source as UTF-16 and stores it in the buffer pointed to by dest
 * as a NULL-terminated string
 * dest must contain at least (source.length() + 1) SQLWCHAR elements
 */
  SQLWCHAR* last_char = utf8::utf8to16(source.begin(), source.end(), dest);
  *last_char++ = 0;  // closing null character

  return;
}

std::string encodeUTF16StringAsUTF8(const SQLWCHAR* const source, const SQLLEN& len) {
  std::string utf8line;
  utf8::utf16to8(source, source + (len / sizeof(SQLWCHAR)), std::back_inserter(utf8line));

  return utf8line;
}

std::shared_ptr<SQLWCHAR> get_UTF16_string(const std::string& source) {
/*
 * C++-11 technique
 */
  /*std::wstring_convert<std::codecvt_utf8_utf16<char16_t>,char16_t> convert;
  std::u16string dest = convert.from_bytes(source);

  Rcpp::Rcout << "Converted string is " << dest << std::endl;
  return (void *) (dest.data());*/

  // Convert it to utf-16
  SQLWCHAR* char_data = new SQLWCHAR[source.length() + 1];
  std::shared_ptr<SQLWCHAR> utf16line(char_data, std::default_delete<SQLWCHAR[]>());

  encodeUTF8StringAsUTF16(utf16line.get(), source);
  return utf16line;
}
#ifdef RDB2_DEBUG

void hexdump(void *mem, unsigned int len, unsigned int HEXDUMP_COLS) {
  unsigned int i, j;

  for (i = 0; i < len + ((len % HEXDUMP_COLS) ? (HEXDUMP_COLS - len % HEXDUMP_COLS) : 0); i++) {
    /* print offset */
    if (i % HEXDUMP_COLS == 0) {
      std::cout << std::hex << std::setfill('0') << std::setw(2) << i << ": ";
    }

    /* print hex data */
    if (i < len) {
      std::cout << std::hex << std::setfill('0') << std::setw(2) << (0xFF & ((char*) mem)[i]) << " ";
    } else /* end of block, just aligning for ASCII dump */
    {
      std::cout << "   ";
    }

    /* print ASCII dump */
    if (i % HEXDUMP_COLS == (HEXDUMP_COLS - 1)) {
      for (j = i - (HEXDUMP_COLS - 1); j <= i; j++) {
        if (j >= len) /* end of block, not really printing */
        {
          std::cout << ' ';
        } else if (isprint(((char*) mem)[j])) /* printable char */
        {
          std::cout << (char) (0xFF & ((char*) mem)[j]);
        } else /* other char */
        {
          std::cout << '.';
        }
      }
      std::cout << std::dec << std::endl;
    }
  }
}

#endif
}



