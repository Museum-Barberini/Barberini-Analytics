"use strict";
/**
 * Handles all exceptions that are raised by this method, logs them to the console and raises them
 * again.
 */


export function logExceptions(): MethodDecorator {
    return function (
        _target: Object, _propertyKey: string, descriptor: TypedPropertyDescriptor<any>):
        TypedPropertyDescriptor<any> {
        return {
            value: function () {
                try {
                    return descriptor.value.apply(this, arguments);
                } catch (e) {
                    console.error(e);
                    throw e;
                }
            }
        };
    };
}
