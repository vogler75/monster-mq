/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.rocworks.oa4j.var;

/**
 *
 * @author vogler
 */
public enum VariableType {
     Unknown (0),
     AnyTypeVar (1),
     Bit32Var (2),
     Bit64Var (3),
     BitVar (4),
     BlobVar (5),
     CharVar (6),
     DpIdentifierVar (7),
     DynVar (8),
     ErrorVar (9),
     FloatVar (10),
     IntegerVar (11),
     LangTextVar (12),
     TextVar (13),
     TimeVar (14),
     UIntegerVar (15),
     LongVar (16),
     ULongVar (17),
     NullVar (0xFFFFFFFF);

     VariableType(int n) {
         value=n;
     }
     public final int value;
}