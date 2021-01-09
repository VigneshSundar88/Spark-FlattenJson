from pyspark.sql.types import *
from pyspark.sql.functions import col, posexplode_outer

def flattenDataFrame(explodeDF):
    DFSchema = explodeDF.schema
    fields = DFSchema.fields
    fieldNames = DFSchema.fieldNames()
    fieldLength = len(fieldNames)
	
    for i in range(fieldLength):
        field = fields[i]
	fieldName = field.name
	fieldDataType = field.dataType
		
	if isinstance(fieldDataType, ArrayType):
	    fieldNameExcludingArray = list(filter(lambda colName: colName != fieldName, fieldNames))
	    fieldNamesAndExplode = fieldNameExcludingArray + ["posexplode_outer({0}) as ({1}, {2})".format(fieldName, fieldName+"_pos", fieldName)]
	    arrayDF = explodeDF.selectExpr(*fieldNamesAndExplode)
	    return flattenDataFrame(arrayDF)
	elif isinstance(fieldDataType, StructType):
	    childFieldnames = fieldDataType.names
	    structFieldNames = list(map(lambda childname: fieldName +"."+childname, childFieldNames))
	    newFieldNames = list(filter(lambda colName: colName != fieldName, fieldNames)) + structFieldNames
	    renamedCols = map(lambda x: x.replace(".", "_"), newFieldNames)
	    zipAliasColNames = zip(newFieldNames, renamedCols)
	    aliasColNames = map(lambda y: col(y[0]).alias(y[1]), zipAliasColNames)
	    structDF = explodeDF.select(*aliasColNames)
	    return flattenDataFrame(structDF)
    return explodeDF
	
readJson = spark.read.json("<json_path>")
flattenDataFrame(readJson)
