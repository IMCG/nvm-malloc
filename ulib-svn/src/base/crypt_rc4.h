/* The MIT License

   Copyright (C) 2011 Zilong Tan (eric.zltan@gmail.com)

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

#ifndef _ULIB_CRYPT_RC4_H
#define _ULIB_CRYPT_RC4_H

#include <stddef.h>
#include <stdint.h>

typedef struct rc4_ks_t {
	uint8_t state[256];
	uint8_t x;
	uint8_t y;
} rc4_ks_t;

#ifdef __cplusplus
extern "C" {
#endif

void rc4_setks(const uint8_t *kbuf, size_t klen, rc4_ks_t *ks);

// in-place encryption/decryption
void rc4_crypt(uint8_t *buf, size_t len, rc4_ks_t *ks);

#ifdef __cplusplus
}
#endif

#endif	/* _ULIB_CRYPT_RC4_H */
