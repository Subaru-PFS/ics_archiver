#!/usr/bin/env python
"""
Unit tests for archiver.expression
"""

# Created 29-Jul-2009 by David Kirkby (dkirkby@uci.edu)

import unittest
import math
import archiver.expression as expr

class ExpressionTests(unittest.TestCase):

    def setUp(self):
        self.p = expr.Parser()
        
    def pValue(self,expr):
        return self.p.parse(expr).value

    def test00(self):
        "Valid arithmetic expressions"
        self.failUnless(self.p.parse("0B10101"))
        self.failUnless(self.p.parse("0xdeadbeef"))
        self.failUnless(self.p.parse("3.141e-0"))
        self.failUnless(self.p.parse("-1"))
        self.failUnless(self.p.parse("1+1"))
        self.failUnless(self.p.parse("1+-1"))
        self.failUnless(self.p.parse("1*2*3-4/5"))

    def test01(self):
        "Invalid arithmetic expressions"
        self.assertRaises(expr.ExpressionError,lambda: self.p.parse("-"))
        self.assertRaises(expr.ExpressionError,lambda: self.p.parse(".1."))
        
    def test02(self):
        "Valid where expressions"
        self.assertEqual(self.pValue("1 when 0"),None)
        self.assertEqual(self.pValue("1 when 1"),1)

    def test03(self):
        "Invalid where expressions"
        self.assertRaises(expr.ExpressionError,lambda: self.p.parse("a where b where c"))
        self.assertRaises(expr.ExpressionError,lambda: self.p.parse("f(a where b)"))
        
    def test04(self):
        "Numeric constants"
        self.assertEqual(self.pValue("123"),123)
        self.assertEqual(self.pValue("1.23"),1.23)
        self.assertEqual(self.pValue("0xdeadbeef"),0xdeadbeef)
        self.assertEqual(self.pValue("0B1101"),13)
        
    def test05(self):
        "Unary expressions"
        self.assertEqual(self.pValue("-123"),-123)
        self.assertEqual(self.pValue("+123"),123)
        self.assertEqual(self.pValue("-+123"),-123)
        self.assertEqual(self.pValue("-(+(123))"),-123)
        self.assertEqual(self.pValue("!0"),True)
        self.assertEqual(self.pValue("!1"),False)
        
    def test06(self):
        "Parentheses"
        self.assertEqual(self.pValue("-(+(123))"),-123)
        self.assertRaises(expr.ExpressionError,lambda: self.pValue("(1"))
        self.assertRaises(expr.ExpressionError,lambda: self.pValue("(1))"))
        self.assertRaises(expr.ExpressionError,lambda: self.pValue("()"))
    
    def test07(self):
        "Binary expressions"
        self.assertEqual(self.pValue("1+1"),2)
        self.assertEqual(self.pValue("1+-1"),0)
        self.assertEqual(self.pValue("9%2"),1)
        self.assertEqual(self.pValue("1-1+1"),1)
        self.assertEqual(self.pValue("1+2*3"),7)
        self.assertEqual(self.pValue("(1+2*3)/2"),3)
        self.assertEqual(self.pValue("(1+2*3)/2."),3.5)
        
    def test08(self):
        "Relational expressions"
        self.assertEqual(self.pValue("1>0"),True)
        self.assertEqual(self.pValue("!!(1>0)"),True)
        self.assertEqual(self.pValue("1.23 when 1==0"),None)
        self.assertEqual(self.pValue("1.23 when 1!=0"),1.23)
        self.assertEqual(self.pValue("0.9 >= -1"),True)

    def test09(self):
        "Logical expressions"
        self.assertEqual(self.pValue("1 && 1"),True)
        self.assertEqual(self.pValue("1 && 0"),False)
        self.assertEqual(self.pValue("1 || 0"),True)
        self.assertEqual(self.pValue("0 || 0"),False)
        
    def test10(self):
        "Conditional expressions"
        self.assertEqual(self.pValue("1 ? 2 : 3"),2)
        self.assertEqual(self.pValue("0 ? 2 : 3"),3)
        
    def test11(self):
        "Function calls"
        self.assertEqual(self.pValue("sin(1.23)"),math.sin(1.23))
        self.assertEqual(self.pValue("atan2(0,-1)"),math.pi)
        self.assertRaises(expr.ExpressionError,lambda: self.pValue("sin(1,2)"))
        self.assertRaises(expr.ExpressionError,lambda: self.pValue("atan2(0)"))
        
    def test12(self):
        "String literals"
        self.assertEqual(self.pValue('"hello, world"'),'hello, world')
        self.assertEqual(self.pValue("'hello, world'"),'hello, world')
        self.assertEqual(self.pValue('"don\'t run"'),"don't run")
        self.assertEqual(self.pValue(r"'don\'t run'"),r'don\'t run')
        
    def test13(self):
        "String expressions"
        self.assertEqual(self.pValue("'hello'=='hello'"),True)
        self.assertEqual(self.pValue("'hello, world' == 'hello,' + ' world'"),True)
    
    def test14(self):
        "Named constants"
        self.assertEqual(self.pValue('true'),True)
        self.assertEqual(self.pValue('pi+e'),math.pi+math.e)
        self.assertEqual(self.pValue('sin(0.5*pi)'),1)
        self.assertRaises(expr.ExpressionError,lambda: self.pValue("epi"))
        
    def test15(self):
        "KeyValue nodes"
        self.assertEqual(self.pValue('a.b'),None)
        self.assertEqual(self.pValue('a.b.c'),None)
        self.assertEqual(self.pValue('a.b.c+x.y'),None)
        
    def test16(self):
        "KeyValue updates"
        expr = self.p.parse("x.y.val0 + pow(a.b.val2,x.y.val2)")
        self.assertEqual(expr.value,None)
        self.assertEqual(expr.update("a.b",{'val0':0,'val1':1,'val2':2}),True)
        self.assertEqual(expr.value,None)
        self.assertEqual(expr.update("x.y",{'val0':9,'val1':8,'val2':7}),True)
        self.assertEqual(expr.value,9+math.pow(2,7))
        
    def test17(self):
        "KeyValue updates with when clause"
        expr1 = self.p.parse("x.y.val when a.b.val")
        self.assertEqual(expr1.value,None)
        self.assertEqual(expr1.update("a.b",{'val':False}),False)
        self.assertEqual(expr1.value,None)
        self.assertEqual(expr1.update("x.y",{'val':999}),False)
        self.assertEqual(expr1.value,None)
        self.assertEqual(expr1.update("a.b",{'val':True}),True)
        self.assertEqual(expr1.value,999)
        self.assertEqual(expr1.update("x.y",{'val':123}),True)
        self.assertEqual(expr1.value,123)
        expr2 = self.p.parse("x.y.val when a.b.val")
        self.assertEqual(expr2.value,None)
        self.assertEqual(expr2.update("a.b",{'val':True}),True)
        self.assertEqual(expr2.value,None)
        self.assertEqual(expr2.update("x.y",{'val':999}),True)
        self.assertEqual(expr2.value,999)
        self.assertEqual(expr2.update("a.b",{'val':False}),False)
        self.assertEqual(expr2.value,999)
        self.assertEqual(expr2.update("x.y",{'val':123}),False)
        self.assertEqual(expr2.value,999)
    
    def test18(self):
        "Builtin function calls"
        self.assertEqual(self.pValue("max(1,2)"),2)
        self.assertEqual(self.pValue("round(pi,5)"),round(math.pi,5))
        self.assertEqual(self.pValue("int('1101',2)"),13)

if __name__ == '__main__':
    unittest.main()