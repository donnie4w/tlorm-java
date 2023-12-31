/*
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 * https://githuc.com/donnie4w/tldb
 * https://githuc.com/donnie4w/tlorm-java
 */
package io.github.donnie4w.tlorm;

import java.lang.annotation.*;

@Target(ElementType.FIELD)
@Retention(value= RetentionPolicy.RUNTIME)
public @interface Index {
    String  idxName() default "";
}
