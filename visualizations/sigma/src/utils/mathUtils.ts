import {Complex} from 'mathjs';
// TODO: It would be nicer to declare extension methods here, but TypeScript
// appears not to support extensions on interface at the moment.


export function deconstructPolar(complex: Complex) {
    const polar = complex.toPolar();
    return [polar.phi, polar.r];
}
