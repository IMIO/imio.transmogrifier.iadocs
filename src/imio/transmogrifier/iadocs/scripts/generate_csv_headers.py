# -*- coding: utf-8 -*-
from itertools import product
import argparse

letters = ('', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
           'V', 'W', 'X', 'Y', 'Z')

parser = argparse.ArgumentParser(description='Generate csv headers.')
parser.add_argument('last_one', help='Last wanted header as W, AC. (without prefix)')
parser.add_argument('-p', '--prefix', dest='prefix', help='Prefix.', default='_')
parser.add_argument('-s', '--sep', dest='separator', help='Output separator.', default=' ')
ns = parser.parse_args()

res = []
for let1, let2 in product(letters, letters[1:]):
    gen = '{}{}{}'.format(ns.prefix, let1, let2)
    res.append(gen)
    if gen == '{}{}'.format(ns.prefix, ns.last_one):
        break
print(ns.separator.join(res))
