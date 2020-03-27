import {SigmaV01} from '../js/sigma';
// TODO: It would be nicer to declare extension methods here, but TypeScript
// appears not to support extensions on interface at the moment.

export function clean(sigma: SigmaV01.Sigma) {
	sigma._core.graph.empty()
}
