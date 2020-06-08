#!/usr/bin/env python

from distutils.core import setup

setup(name='hipop',
      version='2.0',
      description='Hierarchical Partial-Order Planner',
      author='Charles Lesire',
      author_email='charles.lesire@onera.fr',
      author='Alexandre Albore',
      author_email='alexandre.albore@onera.fr',
      packages=['hipop', 'hipop.search', 'hipop.utils'],
     )
