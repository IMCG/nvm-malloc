/* The MIT License

   Copyright (C) 2013  Zilong Tan (eric.zltan@gmail.com)
   Copyright (C) 2009, 2011 by Attractive Chaos <attractor@live.co.uk>

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

#ifndef _ULIB_MATH_RNG_GAMMA_H
#define _ULIB_MATH_RNG_GAMMA_H

#include <stdint.h>
#include "math_rng_normal.h"

typedef struct normal_rng gamma_rng_t;

#ifdef __cplusplus
extern "C" {
#endif

void gamma_rng_init(gamma_rng_t *rng);

/* g(x;a,b) = x^{a-1} b^a e^{-bx} / \Gamma(a) */
double gamma_rng_next(gamma_rng_t *rng, double alpha, double beta);

#ifdef __cplusplus
}
#endif

#endif  /* _ULIB_MATH_RNG_GAMMA_H */
