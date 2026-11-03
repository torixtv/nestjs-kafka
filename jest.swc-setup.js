'use strict';
// SWC emits CommonJS live-binding getters with configurable: false.
// jest.spyOn on `import * as mod` then throws "Cannot redefine property".
const defineProperty = Object.defineProperty;
Object.defineProperty = function (object, property, descriptor) {
  if (descriptor && (descriptor.get || descriptor.set) && descriptor.configurable !== true) {
    descriptor = { ...descriptor, configurable: true };
  }
  return defineProperty(object, property, descriptor);
};
