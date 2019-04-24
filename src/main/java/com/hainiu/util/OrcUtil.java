package com.hainiu.util;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;

public class OrcUtil  {

    /**
     * 读orc文件的inspector
     */
    StructObjectInspector inspector_r = null;

    /**
     * 写orc文件的inspector
     */
    StructObjectInspector inspector_w = null;

    /**
     * 序列化前，存储一行数据
     */
    List<Object> realRow = null;


    /**
     * orc 文件的序列化类
     */
    OrcSerde serde = null;

    /**
     * 设置读取 orc 文件的inspector对象
     */
    public void setOrcTypeReadSchema(){
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(OrcFormat.TYPE);
        inspector_r = (StructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
    }

    /**
     * 设置写orc文件的inspector对象
     */
    public void setOrcTypeWriteSchema(){
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(OrcFormat.TYPE);
        inspector_w = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    }

    /**
     * 根据字段名称获取字段值
     * @param orcStruct 带有数据对象
     * @param fieldName 字段名称
     * @return 该字段的值
     */
    public String getOrcData(OrcStruct orcStruct, String fieldName){
        // 根据字段名称获取struct 里面的字段对象
        StructField structField = inspector_r.getStructFieldRef(fieldName);
        // 根据字段对象获获取去 orcStruct 里面的数据
        String data = inspector_r.getStructFieldData(orcStruct , structField).toString();
        data = (data == null || "null".equalsIgnoreCase(data)) ? null : data;
        return data;
    }

    /**
     * 写入orc文件时，添加数据
     * addAttr(aid).addAttr(pkgname, uptime);
     * @param attrs
     * @return 当前对象this
     */
    public OrcUtil addAttr(Object... attrs){
        if (serde == null) {
            serde = new OrcSerde();
        }
        for (Object attr:
             attrs) {
            realRow.add(attr);
        }
        return this;
    }

    public Writable serialize() {
        if (serde == null) {
            serde = new OrcSerde();
        }
        // 序列化数据
        Writable w = serde.serialize(realRow , inspector_w);
        // 序列化后，清空realRow
        realRow = new ArrayList<>();
        return w;
    }



}
