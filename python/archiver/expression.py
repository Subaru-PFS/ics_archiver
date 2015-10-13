"""
Parse archiver expressions

Refer to https://trac.sdss3.org/wiki/Ops/Arch/Expression for details.
"""

# Created 14-Nov-2008 by David Kirkby (dkirkby@uci.edu)

import math
import __builtin__

import external.ply.lex as lex
import external.ply.yacc as yacc

# http://www.lysator.liu.se/c/ANSI-C-grammar-l.html
# http://www.lysator.liu.se/c/ANSI-C-grammar-y.html

class ExpressionError(Exception):
    pass

class Node(object):
    """
    Represents a generic expression node
    """
    def __init__(self,*args):
        self.args = args
        self.parent = None
        self.children = [ ]
        self.watchSet = set()
        self.value = None
        
    def addChild(self,child):
        """
        Adds a node directly below this node
        """
        assert(child.parent is None)
        assert(child not in self.children)
        self.children.append(child)
        self.watchSet.update(child.watchSet)
        child.parent = self
        
    def update(self,keytag,values):
        """
        Updates the values of keytag for this node and its children

        Returns True or False to indicate whether this update changed
        the value of this node's (sub)expression. If any children
        change as a result of this update, calls evaluate to update
        our value.
        """
        keytag = keytag.lower()
        changed = False
        if keytag in self.watchSet:
            for child in self.children:
                if child.update(keytag,values):
                    changed = True
            if changed:
                self.evaluate()
        return changed
        
    def evaluate(self):
        """
        Update this node's value
        
        Subclasses must implement this if they might have child nodes
        """
        raise NotImplementedError
    
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,','.join([repr(arg) for arg in self.args]))

class EvalNode(Node):
    """
    Represents a node that is evaluated via a python expression
    """
    def __init__(self,expr,*args):
        Node.__init__(self,*args)
        self.expr = expr
        self.compiled = compile(expr,'<string>','eval')
        
    translated = { '!':'not', '&&':'and', '||':'or' }

    @staticmethod
    def translate(op):
        return EvalNode.translated[op] if op in EvalNode.translated else op
    
    def validChildren(self):
        """
        Checks that all child nodes have a valid value
        """
        for child in self.children:
            if child.value is None:
                return False
        return True
    
    def evaluate(self):
        self.value = None
        if self.validChildren():
            try:
                self.value = eval(self.compiled)
            except:
                raise ExpressionError('unable to evaluate %s' % str(self))
                
    def __repr__(self):
        return Node.__repr__(self) + ' using %s' % self.expr

class Constant(Node):
    """
    Represents a numeric or string constant
    """
    def __init__(self,value):
        Node.__init__(self,value)
        self.value = value

class Identifier(Node):
    """
    Represents a named constant
    """
    constants = {
        'e': math.e, 'pi': math.pi,
        'true': True, 'false': False
    }
    def __init__(self,name):
        Node.__init__(self,name)
        try:
            self.value = Identifier.constants[name]
        except KeyError:
            raise ExpressionError('Invalid identifier: %s' % name)

class KeyValue(Node):
    """
    Represents a keyword value
    """
    def __init__(self,actorName,keyName,valueItem=None):
        Node.__init__(self,actorName,keyName,valueItem)
        self.keytag = "%s.%s" % (actorName.lower(),keyName.lower())
        self.watchSet.add(self.keytag)
        self.valueItem = valueItem

    def update(self,keytag,values):
        if keytag != self.keytag:
            return False
        try:
            self.value = values[self.valueItem]
        except (KeyError,IndexError):
            raise ExpressionError('Invalid value item: %r' % self.valueItem)
        return True
        
class Unary(EvalNode):
    """
    Represents a unary arithmetic or logical expression
    """
    def __init__(self,op,arg):
        EvalNode.__init__(self,"%s self.children[0].value"
            % EvalNode.translate(op),op,arg)
        self.addChild(arg)
        self.evaluate()    
            
class Binary(EvalNode):
    """
    Represents a binary arithmetic or relational expression
    """
    def __init__(self,arg1,op,arg2):
        EvalNode.__init__(self,"self.children[0].value %s self.children[1].value"
            % EvalNode.translate(op),arg1,op,arg2)
        self.addChild(arg1)
        self.addChild(arg2)
        self.evaluate()

class Conditional(EvalNode):
    """
    Represents a conditional expression X ? Y : Z
    """
    def __init__(self,condExpr,trueExpr,falseExpr):
        EvalNode.__init__(self,
            "self.args[1].value if self.args[0].value else self.args[2].value",
            condExpr,trueExpr,falseExpr)
        self.addChild(condExpr)
        self.addChild(trueExpr)
        self.addChild(falseExpr)
        self.evaluate()

class When(Node):
    """
    Represents a top-level expression: X [ when Y ]
    """
    def __init__(self,valueExpr,whenExpr=None):
        Node.__init__(self,valueExpr,whenExpr)
        self.addChild(valueExpr)
        if whenExpr:
            self.addChild(whenExpr)
        self.valueExpr = valueExpr
        self.whenExpr = whenExpr
        self.evaluate()
        
    def update(self,keytag,values):
        changed = False
        valueChanged = self.valueExpr.update(keytag,values)
        if self.whenExpr:
            if self.whenExpr.update(keytag,values):
                # the when clause has been updated: latch a new value if it is now True
                changed = (self.whenExpr.value == True)
            else:
                # the when clause has not changed
                if self.whenExpr.value == True:
                    # if it is still True, pass through any change to our value
                    changed = valueChanged
        else:
            changed = valueChanged
        if changed:
            self.evaluate()
        return changed

    def evaluate(self):
        if self.whenExpr is None or self.whenExpr.value == True:
            self.value = self.valueExpr.value

class Call(EvalNode):
    """
    Represents a function call
    """
    builtins = ('abs','int','float','max','min','round')
    
    def __init__(self,funcName,argList):
        pyCall = None
        # is this a math function?
        if funcName[0:2] != '__' and funcName in math.__dict__:
            pyCall = 'math.%s' % funcName
        # is this a builtin function that we expose?
        elif funcName in Call.builtins:
            pyCall = '__builtin__.%s' % funcName
        if not pyCall:
            raise ExpressionError('Unknown function: %s' % funcName)
        pyArgs = ','.join(["self.children[%d].value" % k for k in range(len(argList))])
        EvalNode.__init__(self,"%s(%s)" % (pyCall,pyArgs),funcName,*argList)
        # cannot add children until our superclass ctor has been called
        for child in argList:
            self.addChild(child)
        self.evaluate()

    def __repr__(self):
        return EvalNode.__repr__(self) + ' args %r' % [arg.value for arg in self.args[1:]]

class Parser(object):
    """
    A combined lexer and parser for archiver expressions
    """
    debug = False
    
    # single-character literals
    literals = "()*/%+-,<>!?:."
    
    # ignore inline whitespace between tokens
    t_ignore = ' \t\n'
    
    # lexical tokens
    tokens = (
        'IDENTIFIER','BINCONST','HEXCONST','DECCONST','FLTCONST',
        'STRINGLIT1','STRINGLIT2','AND','OR','EQ','NE','LEQ','GEQ','WHEN'
    )
    
    t_BINCONST = r'0[bB][01]+'
    t_HEXCONST = r'0[xX][a-fA-F0-9]+'
    t_DECCONST = r'[0-9]+'
    t_FLTCONST = r'([0-9]+[Ee][+-]?[0-9]+)|([0-9]*\.[0-9]+([Ee][+-]?[0-9]+)?)|([0-9]+\.[0-9]*([Ee][+-]?[0-9]+)?)'
    t_AND = '&&'
    t_OR = '\|\|'
    t_EQ = '=='
    t_NE = '!='
    t_LEQ = '<='
    t_GEQ = '>='
    # String literals can be enclosed in either 'single' or "double" quotes
    # An embedded quote character must be escaped via \' or \"
    # Multiline strings and multicharacter escapes are not supported
    t_STRINGLIT1 = r'"(\\.|[^"])*"'
    t_STRINGLIT2 = r"'(\\.|[^'])*'"
    
    def t_IDENTIFIER(self,t):
        r'[a-zA-Z_][a-zA-Z_0-9]*'
        if t.value.lower() == 'when':
            t.type = 'WHEN'
        return t
    
    def p_primary_expression_1(self,p):
        "primary_expression : IDENTIFIER"
        p[0] = Identifier(p[1])
        
    def p_primary_expression_2(self,p):
        "primary_expression : BINCONST"
        # strip off the leading 0b for backwards compatibility with python < 2.6
        p[0] = Constant(int(p[1][2:],2))

    def p_primary_expression_3(self,p):
        "primary_expression : HEXCONST"
        p[0] = Constant(int(p[1],16))
    
    def p_primary_expression_4(self,p):
        "primary_expression : DECCONST"
        p[0] = Constant(int(p[1]))
        
    def p_primary_expression_5(self,p):
        "primary_expression : FLTCONST"
        p[0] = Constant(float(p[1]))
    
    def p_primary_expression_6(self,p):
        "primary_expression : STRINGLIT1"
        # drop the quotes to extract the string text
        p[0] = Constant(str(p[1][1:-1]))
        
    def p_primary_expression_7(self,p):
        "primary_expression : STRINGLIT2"
        # drop the quotes to extract the string text
        p[0] = Constant(str(p[1][1:-1]))

    def p_primary_expression_8(self,p):
        "primary_expression : '(' expression ')'"
        p[0] = p[2]
        
    def p_postfix_expression_1(self,p):
        "postfix_expression : primary_expression"
        p[0] = p[1]

    def p_postfix_expression_2(self,p):
        "postfix_expression : IDENTIFIER '(' argument_expression_list ')'"
        p[0] = Call(p[1],p[3])
        
    def p_postfix_expression_3(self,p):
        "postfix_expression : IDENTIFIER '.' IDENTIFIER"
        p[0] = KeyValue(p[1],p[3])

    def p_postfix_expression_4(self,p):
        "postfix_expression : IDENTIFIER '.' IDENTIFIER '.' IDENTIFIER"
        p[0] = KeyValue(p[1],p[3],p[5])

    def p_unary_expression_1(self,p):
        "unary_expression : postfix_expression"
        p[0] = p[1]
    
    def p_unary_expression_2(self,p):
        """unary_expression : '+' unary_expression
                            | '-' unary_expression
                            | '!' unary_expression"""
        p[0] = Unary(p[1],p[2])

    def p_multiplicative_expression_1(self,p):
        "multiplicative_expression : unary_expression"
        p[0] = p[1]

    def p_multiplicative_expression_2(self,p):
        """multiplicative_expression : multiplicative_expression '*' unary_expression
                                     | multiplicative_expression '/' unary_expression
                                     | multiplicative_expression '%' unary_expression"""
        p[0] = Binary(p[1],p[2],p[3])

    def p_additive_expression_1(self,p):
        "additive_expression : multiplicative_expression"
        p[0] = p[1]

    def p_additive_expression_2(self,p):
        """additive_expression : additive_expression '+' multiplicative_expression
                               | additive_expression '-' multiplicative_expression"""
        p[0] = Binary(p[1],p[2],p[3])
        
    def p_relational_expression_1(self,p):
        "relational_expression : additive_expression"
        p[0] = p[1]

    def p_relational_expression_2(self,p):
        """relational_expression : relational_expression '<' additive_expression
                                 | relational_expression '>' additive_expression
                                 | relational_expression LEQ additive_expression
                                 | relational_expression GEQ additive_expression"""
        p[0] = Binary(p[1],p[2],p[3])
    
    def p_equality_expression_1(self,p):
        "equality_expression : relational_expression"
        p[0] = p[1]

    def p_equality_expression_2(self,p):
        """equality_expression : equality_expression EQ relational_expression
                               | equality_expression NE relational_expression"""
        p[0] = Binary(p[1],p[2],p[3])

    def p_logical_and_expression_1(self,p):
        "logical_and_expression : equality_expression"
        p[0] = p[1]

    def p_logical_and_expression_2(self,p):
        "logical_and_expression : logical_and_expression AND equality_expression"
        p[0] = Binary(p[1],p[2],p[3])

    def p_logical_or_expression_1(self,p):
        "logical_or_expression : logical_and_expression"
        p[0] = p[1]

    def p_logical_or_expression_2(self,p):
        "logical_or_expression : logical_or_expression OR logical_and_expression"
        p[0] = Binary(p[1],p[2],p[3])

    def p_conditional_expression_1(self,p):
        "conditional_expression : logical_or_expression"
        p[0] = p[1]

    def p_conditional_expression_2(self,p):
        "conditional_expression : logical_or_expression '?' expression ':' conditional_expression"
        p[0] = Conditional(p[1],p[3],p[5])

    def p_expression_1(self,p):
        "expression : conditional_expression"
        p[0] = p[1]
        
    def p_when_expression_1(self,p):
        "when_expression : expression"
        p[0] = When(p[1])

    def p_when_expression_2(self,p):
        "when_expression : expression WHEN expression"
        p[0] = When(p[1],p[3])

    def p_argument_expression_list_1(self,p):
        "argument_expression_list : expression"
        p[0] = [p[1]]

    def p_argument_expression_list_2(self,p):
        "argument_expression_list : argument_expression_list ',' expression"
        p[0] = p[1]
        p[0].append(p[3])

    start = 'when_expression'
    
    def p_error(self,tok):
        """
        Handles parse errors
        """
        if not tok:
            raise ExpressionError("Unable to parse expression")
        raise ExpressionError("Unexpected %s token in expression" % tok.type)

    def t_error(self,tok):
        """
        Handles lexical analysis errors
        """
        raise ExpressionError("Unable to split expression into tokens")

    def tokenize(self,string):
        """
        Generates the lexical tokens found in a format string
        """
        self.lexer.input(string)
        tok = self.lexer.token()
        while tok:
            yield tok
            tok = self.lexer.token()

    def parse(self,string):
        """
        Returns the parsed representation of a format string
        """
        return self.engine.parse(string,lexer=self.lexer,debug=self.debug)

    def __init__(self):
        """
        Creates a new keywords format string parser
        """
        self.lexer = lex.lex(object=self,debug=self.debug)
        self.engine = yacc.yacc(module=self,debug=self.debug,write_tables=0)
