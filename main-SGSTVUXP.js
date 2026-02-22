var sC=Object.defineProperty,oC=Object.defineProperties;var aC=Object.getOwnPropertyDescriptors;var H_=Object.getOwnPropertySymbols;var cC=Object.prototype.hasOwnProperty,lC=Object.prototype.propertyIsEnumerable;var z_=(n,e,t)=>e in n?sC(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,ae=(n,e)=>{for(var t in e||={})cC.call(e,t)&&z_(n,t,e[t]);if(H_)for(var t of H_(e))lC.call(e,t)&&z_(n,t,e[t]);return n},et=(n,e)=>oC(n,aC(e));var un=null,Gl=!1,_p=1,uC=null,_n=Symbol("SIGNAL");function je(n){let e=un;return un=n,e}function Wl(){return un}var Fa={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function ka(n){if(Gl)throw new Error("");if(un===null)return;un.consumerOnSignalRead(n);let e=un.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=un.recomputing;if(i&&(t=e!==void 0?e.nextProducer:un.producers,t!==void 0&&t.producer===n)){un.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===un&&(!i||fC(r,un)))return;let s=ho(un),o={producer:n,consumer:un,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};un.producersTail=o,e!==void 0?e.nextProducer=o:un.producers=o,s&&$_(n,o)}function G_(){_p++}function xp(n){if(!(ho(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===_p)){if(!n.producerMustRecompute(n)&&!ql(n)){yp(n);return}n.producerRecomputeValue(n),yp(n)}}function Mp(n){if(n.consumers===void 0)return;let e=Gl;Gl=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||dC(i)}}finally{Gl=e}}function bp(){return un?.consumerAllowSignalWrites!==!1}function dC(n){n.dirty=!0,Mp(n),n.consumerMarkedDirty?.(n)}function yp(n){n.dirty=!1,n.lastCleanEpoch=_p}function Ua(n){return n&&j_(n),je(n)}function j_(n){n.producersTail=void 0,n.recomputing=!0}function $l(n,e){je(e),n&&W_(n)}function W_(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(ho(n))do t=Ep(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function ql(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(xp(t),i!==t.version))return!0}return!1}function Ba(n){if(ho(n)){let e=n.producers;for(;e!==void 0;)e=Ep(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function $_(n,e){let t=n.consumersTail,i=ho(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)$_(r.producer,r)}function Ep(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!ho(e)){let s=e.producers;for(;s!==void 0;)s=Ep(s)}return t}function ho(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function Sp(n){uC?.(n)}function fC(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function wp(n,e){return Object.is(n,e)}function Xl(n,e){let t=Object.create(hC);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(xp(t),ka(t),t.value===jl)throw t.error;return t.value};return i[_n]=t,Sp(t),i}var gp=Symbol("UNSET"),vp=Symbol("COMPUTING"),jl=Symbol("ERRORED"),hC=et(ae({},Fa),{value:gp,dirty:!0,error:null,equal:wp,kind:"computed",producerMustRecompute(n){return n.value===gp||n.value===vp},producerRecomputeValue(n){if(n.value===vp)throw new Error("");let e=n.value;n.value=vp;let t=Ua(n),i,r=!1;try{i=n.computation(),je(null),r=e!==gp&&e!==jl&&i!==jl&&n.equal(e,i)}catch(s){i=jl,n.error=s}finally{$l(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function pC(){throw new Error}var q_=pC;function X_(n){q_(n)}function Cp(n){q_=n}var mC=null;function Dp(n,e){let t=Object.create(Yl);t.value=n,e!==void 0&&(t.equal=e);let i=()=>Y_(t);return i[_n]=t,Sp(t),[i,o=>Va(t,o),o=>Z_(t,o)]}function Y_(n){return ka(n),n.value}function Va(n,e){bp()||X_(n),n.equal(n.value,e)||(n.value=e,gC(n))}function Z_(n,e){bp()||X_(n),Va(n,e(n.value))}var Yl=et(ae({},Fa),{equal:wp,value:void 0,kind:"signal"});function gC(n){n.version++,G_(),Mp(n),mC?.(n)}function Tp(n){let e=je(null);try{return n()}finally{je(e)}}function Be(n){return typeof n=="function"}function po(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Zl=po(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Ha(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var nn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Be(i))try{i()}catch(s){e=s instanceof Zl?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{K_(s)}catch(o){e=e??[],o instanceof Zl?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Zl(e)}}add(e){var t;if(e&&e!==this)if(this.closed)K_(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Ha(t,e)}remove(e){let{_finalizers:t}=this;t&&Ha(t,e),e instanceof n&&e._removeParent(this)}};nn.EMPTY=(()=>{let n=new nn;return n.closed=!0,n})();var Ap=nn.EMPTY;function Kl(n){return n instanceof nn||n&&"closed"in n&&Be(n.remove)&&Be(n.add)&&Be(n.unsubscribe)}function K_(n){Be(n)?n():n.unsubscribe()}var si={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var mo={setTimeout(n,e,...t){let{delegate:i}=mo;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=mo;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Jl(n){mo.setTimeout(()=>{let{onUnhandledError:e}=si;if(e)e(n);else throw n})}function za(){}var J_=Ip("C",void 0,void 0);function Q_(n){return Ip("E",void 0,n)}function e0(n){return Ip("N",n,void 0)}function Ip(n,e,t){return{kind:n,value:e,error:t}}var hs=null;function go(n){if(si.useDeprecatedSynchronousErrorHandling){let e=!hs;if(e&&(hs={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=hs;if(hs=null,t)throw i}}else n()}function t0(n){si.useDeprecatedSynchronousErrorHandling&&hs&&(hs.errorThrown=!0,hs.error=n)}var ps=class extends nn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Kl(e)&&e.add(this)):this.destination=_C}static create(e,t,i){return new vo(e,t,i)}next(e){this.isStopped?Np(e0(e),this):this._next(e)}error(e){this.isStopped?Np(Q_(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Np(J_,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},vC=Function.prototype.bind;function Rp(n,e){return vC.call(n,e)}var Pp=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Ql(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Ql(i)}else Ql(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Ql(t)}}},vo=class extends ps{constructor(e,t,i){super();let r;if(Be(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&si.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Rp(e.next,s),error:e.error&&Rp(e.error,s),complete:e.complete&&Rp(e.complete,s)}):r=e}this.destination=new Pp(r)}};function Ql(n){si.useDeprecatedSynchronousErrorHandling?t0(n):Jl(n)}function yC(n){throw n}function Np(n,e){let{onStoppedNotification:t}=si;t&&mo.setTimeout(()=>t(n,e))}var _C={closed:!0,next:za,error:yC,complete:za};var yo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function oi(n){return n}function Op(...n){return Lp(n)}function Lp(n){return n.length===0?oi:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var rt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=MC(t)?t:new vo(t,i,r);return go(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=n0(i),new i((r,s)=>{let o=new vo({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[yo](){return this}pipe(...t){return Lp(t)(this)}toPromise(t){return t=n0(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function n0(n){var e;return(e=n??si.Promise)!==null&&e!==void 0?e:Promise}function xC(n){return n&&Be(n.next)&&Be(n.error)&&Be(n.complete)}function MC(n){return n&&n instanceof ps||xC(n)&&Kl(n)}function bC(n){return Be(n?.lift)}function ct(n){return e=>{if(bC(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function lt(n,e,t,i,r){return new Fp(n,e,t,i,r)}var Fp=class extends ps{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var i0=po(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Vt=(()=>{class n extends rt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new eu(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new i0}next(t){go(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){go(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){go(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?Ap:(this.currentObservers=null,s.push(t),new nn(()=>{this.currentObservers=null,Ha(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new rt;return t.source=this,t}}return n.create=(e,t)=>new eu(e,t),n})(),eu=class extends Vt{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Ap}};var rn=class extends Vt{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};var sn=new rt(n=>n.complete());function r0(n){return n&&Be(n.schedule)}function s0(n){return n[n.length-1]}function tu(n){return Be(s0(n))?n.pop():void 0}function Tr(n){return r0(s0(n))?n.pop():void 0}function a0(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{l(i.next(u))}catch(d){o(d)}}function c(u){try{l(i.throw(u))}catch(d){o(d)}}function l(u){u.done?s(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function o0(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function ms(n){return this instanceof ms?(this.v=n,this):new ms(n)}function c0(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(h){return function(g){return Promise.resolve(g).then(h,d)}}function a(h,g){i[h]&&(r[h]=function(_){return new Promise(function(m,p){s.push([h,_,m,p])>1||c(h,_)})},g&&(r[h]=g(r[h])))}function c(h,g){try{l(i[h](g))}catch(_){f(s[0][3],_)}}function l(h){h.value instanceof ms?Promise.resolve(h.value.v).then(u,d):f(s[0][2],h)}function u(h){c("next",h)}function d(h){c("throw",h)}function f(h,g){h(g),s.shift(),s.length&&c(s[0][0],s[0][1])}}function l0(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof o0=="function"?o0(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,c){o=n[s](o),r(a,c,o.done,o.value)})}}function r(s,o,a,c){Promise.resolve(c).then(function(l){s({value:l,done:a})},o)}}var nu=n=>n&&typeof n.length=="number"&&typeof n!="function";function iu(n){return Be(n?.then)}function ru(n){return Be(n[yo])}function su(n){return Symbol.asyncIterator&&Be(n?.[Symbol.asyncIterator])}function ou(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function EC(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var au=EC();function cu(n){return Be(n?.[au])}function lu(n){return c0(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield ms(t.read());if(r)return yield ms(void 0);yield yield ms(i)}}finally{t.releaseLock()}})}function uu(n){return Be(n?.getReader)}function kt(n){if(n instanceof rt)return n;if(n!=null){if(ru(n))return SC(n);if(nu(n))return wC(n);if(iu(n))return CC(n);if(su(n))return u0(n);if(cu(n))return DC(n);if(uu(n))return TC(n)}throw ou(n)}function SC(n){return new rt(e=>{let t=n[yo]();if(Be(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function wC(n){return new rt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function CC(n){return new rt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,Jl)})}function DC(n){return new rt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function u0(n){return new rt(e=>{AC(n,e).catch(t=>e.error(t))})}function TC(n){return u0(lu(n))}function AC(n,e){var t,i,r,s;return a0(this,void 0,void 0,function*(){try{for(t=l0(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function Dn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function du(n,e=0){return ct((t,i)=>{t.subscribe(lt(i,r=>Dn(i,n,()=>i.next(r),e),()=>Dn(i,n,()=>i.complete(),e),r=>Dn(i,n,()=>i.error(r),e)))})}function fu(n,e=0){return ct((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function d0(n,e){return kt(n).pipe(fu(e),du(e))}function f0(n,e){return kt(n).pipe(fu(e),du(e))}function h0(n,e){return new rt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function p0(n,e){return new rt(t=>{let i;return Dn(t,e,()=>{i=n[au](),Dn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>Be(i?.return)&&i.return()})}function hu(n,e){if(!n)throw new Error("Iterable cannot be null");return new rt(t=>{Dn(t,e,()=>{let i=n[Symbol.asyncIterator]();Dn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function m0(n,e){return hu(lu(n),e)}function g0(n,e){if(n!=null){if(ru(n))return d0(n,e);if(nu(n))return h0(n,e);if(iu(n))return f0(n,e);if(su(n))return hu(n,e);if(cu(n))return p0(n,e);if(uu(n))return m0(n,e)}throw ou(n)}function Ut(n,e){return e?g0(n,e):kt(n)}function tt(...n){let e=Tr(n);return Ut(n,e)}function kp(n,e){let t=Be(n)?n:()=>n,i=r=>r.error(t());return new rt(e?r=>e.schedule(i,0,r):i)}function pu(n){return!!n&&(n instanceof rt||Be(n.lift)&&Be(n.subscribe))}var gs=po(n=>function(){n(this),this.name="EmptyError",this.message="no elements in sequence"});function Dt(n,e){return ct((t,i)=>{let r=0;t.subscribe(lt(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:IC}=Array;function RC(n,e){return IC(e)?n(...e):n(e)}function mu(n){return Dt(e=>RC(n,e))}var{isArray:NC}=Array,{getPrototypeOf:PC,prototype:OC,keys:LC}=Object;function gu(n){if(n.length===1){let e=n[0];if(NC(e))return{args:e,keys:null};if(FC(e)){let t=LC(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function FC(n){return n&&typeof n=="object"&&PC(n)===OC}function vu(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Up(...n){let e=Tr(n),t=tu(n),{args:i,keys:r}=gu(n);if(i.length===0)return Ut([],e);let s=new rt(kC(i,e,r?o=>vu(r,o):oi));return t?s.pipe(mu(t)):s}function kC(n,e,t=oi){return i=>{v0(e,()=>{let{length:r}=n,s=new Array(r),o=r,a=r;for(let c=0;c<r;c++)v0(e,()=>{let l=Ut(n[c],e),u=!1;l.subscribe(lt(i,d=>{s[c]=d,u||(u=!0,a--),a||i.next(t(s.slice()))},()=>{--o||i.complete()}))},i)},i)}}function v0(n,e,t){n?Dn(t,n,e):e()}function y0(n,e,t,i,r,s,o,a){let c=[],l=0,u=0,d=!1,f=()=>{d&&!c.length&&!l&&e.complete()},h=_=>l<i?g(_):c.push(_),g=_=>{s&&e.next(_),l++;let m=!1;kt(t(_,u++)).subscribe(lt(e,p=>{r?.(p),s?h(p):e.next(p)},()=>{m=!0},void 0,()=>{if(m)try{for(l--;c.length&&l<i;){let p=c.shift();o?Dn(e,o,()=>g(p)):g(p)}f()}catch(p){e.error(p)}}))};return n.subscribe(lt(e,h,()=>{d=!0,f()})),()=>{a?.()}}function xn(n,e,t=1/0){return Be(e)?xn((i,r)=>Dt((s,o)=>e(i,s,r,o))(kt(n(i,r))),t):(typeof e=="number"&&(t=e),ct((i,r)=>y0(i,r,n,t)))}function _0(n=1/0){return xn(oi,n)}function x0(){return _0(1)}function _o(...n){return x0()(Ut(n,Tr(n)))}function Ga(n){return new rt(e=>{kt(n()).subscribe(e)})}function Bp(...n){let e=tu(n),{args:t,keys:i}=gu(n),r=new rt(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),c=o,l=o;for(let u=0;u<o;u++){let d=!1;kt(t[u]).subscribe(lt(s,f=>{d||(d=!0,l--),a[u]=f},()=>c--,void 0,()=>{(!c||!d)&&(l||s.next(i?vu(i,a):a),s.complete())}))}});return e?r.pipe(mu(e)):r}function Ji(n,e){return ct((t,i)=>{let r=0;t.subscribe(lt(i,s=>n.call(e,s,r++)&&i.next(s)))})}function ja(n){return ct((e,t)=>{let i=null,r=!1,s;i=e.subscribe(lt(t,void 0,void 0,o=>{s=kt(n(o,ja(n)(e))),i?(i.unsubscribe(),i=null,s.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,s.subscribe(t))})}function yu(n,e){return Be(e)?xn(n,e,1):xn(n,1)}function M0(n){return ct((e,t)=>{let i=!1;e.subscribe(lt(t,r=>{i=!0,t.next(r)},()=>{i||t.next(n),t.complete()}))})}function Qi(n){return n<=0?()=>sn:ct((e,t)=>{let i=0;e.subscribe(lt(t,r=>{++i<=n&&(t.next(r),n<=i&&t.complete())}))})}function b0(n=UC){return ct((e,t)=>{let i=!1;e.subscribe(lt(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(n())))})}function UC(){return new gs}function Vp(n){return ct((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function er(n,e){let t=arguments.length>=2;return i=>i.pipe(n?Ji((r,s)=>n(r,s,i)):oi,Qi(1),t?M0(e):b0(()=>new gs))}function _u(n){return n<=0?()=>sn:ct((e,t)=>{let i=[];e.subscribe(lt(t,r=>{i.push(r),n<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function Hp(...n){let e=Tr(n);return ct((t,i)=>{(e?_o(n,t,e):_o(n,t)).subscribe(i)})}function tr(n,e){return ct((t,i)=>{let r=null,s=0,o=!1,a=()=>o&&!r&&i.complete();t.subscribe(lt(i,c=>{r?.unsubscribe();let l=0,u=s++;kt(n(c,u)).subscribe(r=lt(i,d=>i.next(e?e(c,d,u,l++):d),()=>{r=null,a()}))},()=>{o=!0,a()}))})}function Wa(n){return ct((e,t)=>{kt(n).subscribe(lt(t,()=>t.complete(),za)),!t.closed&&e.subscribe(t)})}function Ei(n,e,t){let i=Be(n)||e||t?{next:n,error:e,complete:t}:n;return i?ct((r,s)=>{var o;(o=i.subscribe)===null||o===void 0||o.call(i);let a=!0;r.subscribe(lt(s,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),s.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),s.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),s.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):oi}var zp;function xu(){return zp}function Si(n){let e=zp;return zp=n,e}var E0=Symbol("NotFound");function xo(n){return n===E0||n?.name==="\u0275NotFound"}var im="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",Ce=class extends Error{code;constructor(e,t){super(Ka(e,t)),this.code=e}};function HC(n){return`NG0${Math.abs(n)}`}function Ka(n,e){return`${HC(n)}${e?": "+e:""}`}function ut(n){for(let e in n)if(n[e]===ut)return e;throw Error("")}function T0(n,e){for(let t in e)e.hasOwnProperty(t)&&!n.hasOwnProperty(t)&&(n[t]=e[t])}function ir(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(ir).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function rm(n,e){return n?e?`${n} ${e}`:n:e||""}var zC=ut({__forward_ref__:ut});function sr(n){return n.__forward_ref__=sr,n.toString=function(){return ir(this())},n}function on(n){return sm(n)?n():n}function sm(n){return typeof n=="function"&&n.hasOwnProperty(zC)&&n.__forward_ref__===sr}function Oe(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function or(n){return{providers:n.providers||[],imports:n.imports||[]}}function Ja(n){return GC(n,wu)}function om(n){return Ja(n)!==null}function GC(n,e){return n.hasOwnProperty(e)&&n[e]||null}function jC(n){let e=n?.[wu]??null;return e||null}function jp(n){return n&&n.hasOwnProperty(bu)?n[bu]:null}var wu=ut({\u0275prov:ut}),bu=ut({\u0275inj:ut}),Te=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Oe({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function am(n){return n&&!!n.\u0275providers}var cm=ut({\u0275cmp:ut}),lm=ut({\u0275dir:ut}),um=ut({\u0275pipe:ut}),dm=ut({\u0275mod:ut}),qa=ut({\u0275fac:ut}),bs=ut({__NG_ELEMENT_ID__:ut}),S0=ut({__NG_ENV_ID__:ut});function fm(n){return Cu(n,"@NgModule"),n[dm]||null}function Ir(n){return Cu(n,"@Component"),n[cm]||null}function hm(n){return Cu(n,"@Directive"),n[lm]||null}function A0(n){return Cu(n,"@Pipe"),n[um]||null}function Cu(n,e){if(n==null)throw new Ce(-919,!1)}function I0(n){return typeof n=="string"?n:n==null?"":String(n)}var R0=ut({ngErrorCode:ut}),WC=ut({ngErrorMessage:ut}),$C=ut({ngTokenPath:ut});function pm(n,e){return N0("",-200,e)}function Du(n,e){throw new Ce(-201,!1)}function N0(n,e,t){let i=new Ce(e,n);return i[R0]=e,i[WC]=n,t&&(i[$C]=t),i}function qC(n){return n[R0]}var Wp;function P0(){return Wp}function Nn(n){let e=Wp;return Wp=n,e}function mm(n,e,t){let i=Ja(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Du(n,"")}var XC={},vs=XC,YC="__NG_DI_FLAG__",$p=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=ys(t)||0;try{return this.injector.get(e,i&8?null:vs,i)}catch(r){if(xo(r))return r;throw r}}};function ZC(n,e=0){let t=xu();if(t===void 0)throw new Ce(-203,!1);if(t===null)return mm(n,void 0,e);{let i=KC(e),r=t.retrieve(n,i);if(xo(r)){if(i.optional)return null;throw r}return r}}function qe(n,e=0){return(P0()||ZC)(on(n),e)}function ie(n,e){return qe(n,ys(e))}function ys(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function KC(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function qp(n){let e=[];for(let t=0;t<n.length;t++){let i=on(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Ce(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=JC(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(qe(r,s))}else e.push(qe(i))}return e}function JC(n){return n[YC]}function _s(n,e){let t=n.hasOwnProperty(qa);return t?n[qa]:null}function O0(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function L0(n){return n.flat(Number.POSITIVE_INFINITY)}function Tu(n,e){n.forEach(t=>Array.isArray(t)?Tu(t,e):e(t))}function gm(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Qa(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function F0(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function k0(n,e,t){let i=Eo(n,e);return i>=0?n[i|1]=t:(i=~i,F0(n,i,e,t)),i}function Au(n,e){let t=Eo(n,e);if(t>=0)return n[t|1]}function Eo(n,e){return QC(n,e,1)}function QC(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var Rr={},Pn=[],Es=new Te(""),vm=new Te("",-1),ym=new Te(""),Xa=class{get(e,t=vs){if(t===vs){let r=N0("",-201);throw r.name="\u0275NotFound",r}return t}};function So(n){return{\u0275providers:n}}function U0(n){return So([{provide:Es,multi:!0,useValue:n}])}function B0(...n){return{\u0275providers:_m(!0,n),\u0275fromNgModule:!0}}function _m(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return Tu(e,o=>{let a=o;Eu(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&V0(r,s),t}function V0(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];xm(r,s=>{e(s,i)})}}function Eu(n,e,t,i){if(n=on(n),!n)return!1;let r=null,s=jp(n),o=!s&&Ir(n);if(!s&&!o){let c=n.ngModule;if(s=jp(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)Eu(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;Tu(s.imports,u=>{Eu(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&V0(l,e)}if(!a){let l=_s(r)||(()=>new r);e({provide:r,useFactory:l,deps:Pn},r),e({provide:ym,useValue:r,multi:!0},r),e({provide:Es,useValue:()=>qe(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;xm(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function xm(n,e){for(let t of n)am(t)&&(t=t.\u0275providers),Array.isArray(t)?xm(t,e):e(t)}var eD=ut({provide:String,useValue:ut});function H0(n){return n!==null&&typeof n=="object"&&eD in n}function tD(n){return!!(n&&n.useExisting)}function nD(n){return!!(n&&n.useFactory)}function xs(n){return typeof n=="function"}function z0(n){return!!n.useClass}var ec=new Te(""),Mu={},w0={},Gp;function tc(){return Gp===void 0&&(Gp=new Xa),Gp}var zt=class{},Ms=class extends zt{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Yp(e,o=>this.processProvider(o)),this.records.set(vm,Mo(void 0,this)),r.has("environment")&&this.records.set(zt,Mo(void 0,this));let s=this.records.get(ec);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(ym,Pn,{self:!0}))}retrieve(e,t){let i=ys(t)||0;try{return this.get(e,vs,i)}catch(r){if(xo(r))return r;throw r}}destroy(){$a(this),this._destroyed=!0;let e=je(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),je(e)}}onDestroy(e){return $a(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){$a(this);let t=Si(this),i=Nn(void 0),r;try{return e()}finally{Si(t),Nn(i)}}get(e,t=vs,i){if($a(this),e.hasOwnProperty(S0))return e[S0](this);let r=ys(i),s,o=Si(this),a=Nn(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=aD(e)&&Ja(e);u&&this.injectableDefInScope(u)?l=Mo(Xp(e),Mu):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?tc():this.parent;return t=r&8&&t===vs?null:t,c.get(e,t)}catch(c){let l=qC(c);throw l===-200||l===-201?new Ce(l,null):c}finally{Nn(a),Si(o)}}resolveInjectorInitializers(){let e=je(null),t=Si(this),i=Nn(void 0),r;try{let s=this.get(Es,Pn,{self:!0});for(let o of s)o()}finally{Si(t),Nn(i),je(e)}}toString(){let e=[],t=this.records;for(let i of t.keys())e.push(ir(i));return`R3Injector[${e.join(", ")}]`}processProvider(e){e=on(e);let t=xs(e)?e:on(e&&e.provide),i=rD(e);if(!xs(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Mo(void 0,Mu,!0),r.factory=()=>qp(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=je(null);try{if(t.value===w0)throw pm(ir(e));return t.value===Mu&&(t.value=w0,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&oD(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{je(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=on(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Xp(n){let e=Ja(n),t=e!==null?e.factory:_s(n);if(t!==null)return t;if(n instanceof Te)throw new Ce(204,!1);if(n instanceof Function)return iD(n);throw new Ce(204,!1)}function iD(n){if(n.length>0)throw new Ce(204,!1);let t=jC(n);return t!==null?()=>t.factory(n):()=>new n}function rD(n){if(H0(n))return Mo(void 0,n.useValue);{let e=Mm(n);return Mo(e,Mu)}}function Mm(n,e,t){let i;if(xs(n)){let r=on(n);return _s(r)||Xp(r)}else if(H0(n))i=()=>on(n.useValue);else if(nD(n))i=()=>n.useFactory(...qp(n.deps||[]));else if(tD(n))i=(r,s)=>qe(on(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=on(n&&(n.useClass||n.provide));if(sD(n))i=()=>new r(...qp(n.deps));else return _s(r)||Xp(r)}return i}function $a(n){if(n.destroyed)throw new Ce(205,!1)}function Mo(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function sD(n){return!!n.deps}function oD(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function aD(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Yp(n,e){for(let t of n)Array.isArray(t)?Yp(t,e):t&&am(t)?Yp(t.\u0275providers,e):e(t)}function dn(n,e){let t;n instanceof Ms?($a(n),t=n):t=new $p(n);let i,r=Si(t),s=Nn(void 0);try{return e()}finally{Si(r),Nn(s)}}function G0(){return P0()!==void 0||xu()!=null}var ci=0,Fe=1,Ue=2,Zt=3,qn=4,Xn=5,nc=6,wo=7,fn=8,Nr=9,ar=10,Lt=11,Co=12,bm=13,Ss=14,Yn=15,Pr=16,ws=17,wi=18,Or=19,Em=20,nr=21,Iu=22,ic=23,On=24,Ru=25,rc=26,Ln=27,j0=1;var Lr=7,sc=8,Cs=9,bn=10;function Ci(n){return Array.isArray(n)&&typeof n[j0]=="object"}function li(n){return Array.isArray(n)&&n[j0]===!0}function Sm(n){return(n.flags&4)!==0}function Fr(n){return n.componentOffset>-1}function oc(n){return(n.flags&1)===1}function Di(n){return!!n.template}function Do(n){return(n[Ue]&512)!==0}function Ds(n){return(n[Ue]&256)===256}var W0="svg",$0="math";function Zn(n){for(;Array.isArray(n);)n=n[ci];return n}function wm(n,e){return Zn(e[n])}function Ti(n,e){return Zn(e[n.index])}function Nu(n,e){return n.data[e]}function Kn(n,e){let t=e[n];return Ci(t)?t:t[ci]}function q0(n){return(n[Ue]&4)===4}function Pu(n){return(n[Ue]&128)===128}function X0(n){return li(n[Zt])}function ac(n,e){return e==null?null:n[e]}function Cm(n){n[ws]=0}function Dm(n){n[Ue]&1024||(n[Ue]|=1024,Pu(n)&&lc(n))}function Y0(n,e){for(;n>0;)e=e[Ss],n--;return e}function cc(n){return!!(n[Ue]&9216||n[On]?.dirty)}function Ou(n){n[ar].changeDetectionScheduler?.notify(8),n[Ue]&64&&(n[Ue]|=1024),cc(n)&&lc(n)}function lc(n){n[ar].changeDetectionScheduler?.notify(0);let e=Ar(n);for(;e!==null&&!(e[Ue]&8192||(e[Ue]|=8192,!Pu(e)));)e=Ar(e)}function Tm(n,e){if(Ds(n))throw new Ce(911,!1);n[nr]===null&&(n[nr]=[]),n[nr].push(e)}function Z0(n,e){if(n[nr]===null)return;let t=n[nr].indexOf(e);t!==-1&&n[nr].splice(t,1)}function Ar(n){let e=n[Zt];return li(e)?e[Zt]:e}function Am(n){return n[wo]??=[]}function Im(n){return n.cleanup??=[]}function K0(n,e,t,i){let r=Am(e);r.push(t),n.firstCreatePass&&Im(n).push(i,r.length-1)}var Je={lFrame:px(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Zp=!1;function J0(){return Je.lFrame.elementDepthCount}function Q0(){Je.lFrame.elementDepthCount++}function ex(){Je.lFrame.elementDepthCount--}function Rm(){return Je.bindingsEnabled}function tx(){return Je.skipHydrationRootTNode!==null}function nx(n){return Je.skipHydrationRootTNode===n}function ix(){Je.skipHydrationRootTNode=null}function yt(){return Je.lFrame.lView}function Tn(){return Je.lFrame.tView}function Ai(n){return Je.lFrame.contextLView=n,n[fn]}function Ii(n){return Je.lFrame.contextLView=null,n}function Fn(){let n=Nm();for(;n!==null&&n.type===64;)n=n.parent;return n}function Nm(){return Je.lFrame.currentTNode}function rx(){let n=Je.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function To(n,e){let t=Je.lFrame;t.currentTNode=n,t.isParent=e}function Pm(){return Je.lFrame.isParent}function sx(){Je.lFrame.isParent=!1}function Om(){return Zp}function Lm(n){let e=Zp;return Zp=n,e}function ox(){let n=Je.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function ax(n){return Je.lFrame.bindingIndex=n}function Lu(){return Je.lFrame.bindingIndex++}function cx(n){let e=Je.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function lx(){return Je.lFrame.inI18n}function ux(n,e){let t=Je.lFrame;t.bindingIndex=t.bindingRootIndex=n,Fu(e)}function dx(){return Je.lFrame.currentDirectiveIndex}function Fu(n){Je.lFrame.currentDirectiveIndex=n}function fx(n){let e=Je.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Fm(){return Je.lFrame.currentQueryIndex}function ku(n){Je.lFrame.currentQueryIndex=n}function cD(n){let e=n[Fe];return e.type===2?e.declTNode:e.type===1?n[Xn]:null}function km(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=cD(s),r===null||(s=s[Ss],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Je.lFrame=hx();return i.currentTNode=e,i.lView=n,!0}function Uu(n){let e=hx(),t=n[Fe];Je.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function hx(){let n=Je.lFrame,e=n===null?null:n.child;return e===null?px(n):e}function px(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function mx(){let n=Je.lFrame;return Je.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Um=mx;function Bu(){let n=mx();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function gx(n){return(Je.lFrame.contextLView=Y0(n,Je.lFrame.contextLView))[fn]}function Ts(){return Je.lFrame.selectedIndex}function kr(n){Je.lFrame.selectedIndex=n}function Bm(){let n=Je.lFrame;return Nu(n.tView,n.selectedIndex)}function vx(){return Je.lFrame.currentNamespace}var yx=!0;function Vu(){return yx}function Hu(n){yx=n}function Kp(n,e=null,t=null,i){let r=Vm(n,e,t,i);return r.resolveInjectorInitializers(),r}function Vm(n,e=null,t=null,i,r=new Set){let s=[t||Pn,B0(n)];return i=i||(typeof n=="object"?void 0:ir(n)),new Ms(s,e||tc(),i||null,r)}var ai=class n{static THROW_IF_NOT_FOUND=vs;static NULL=new Xa;static create(e,t){if(Array.isArray(e))return Kp({name:""},t,e,"");{let i=e.name??"";return Kp({name:i},e.parent,e.providers,i)}}static \u0275prov=Oe({token:n,providedIn:"any",factory:()=>qe(vm)});static __NG_ELEMENT_ID__=-1},Gt=new Te(""),cr=(()=>{class n{static __NG_ELEMENT_ID__=lD;static __NG_ENV_ID__=t=>t}return n})(),Jp=class extends cr{_lView;constructor(e){super(),this._lView=e}get destroyed(){return Ds(this._lView)}onDestroy(e){let t=this._lView;return Tm(t,e),()=>Z0(t,e)}};function lD(){return new Jp(yt())}var _x=!1,xx=new Te(""),Ur=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new rn(!1);debugTaskTracker=ie(xx,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new rt(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Oe({token:n,providedIn:"root",factory:()=>new n})}return n})(),Qp=class extends Vt{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,G0()&&(this.destroyRef=ie(cr,{optional:!0})??void 0,this.pendingTasks=ie(Ur,{optional:!0})??void 0)}emit(e){let t=je(null);try{super.next(e)}finally{je(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof nn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Ht=Qp;function Su(...n){}function Hm(n){let e,t;function i(){n=Su;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Mx(n){return queueMicrotask(()=>n()),()=>{n=Su}}var zm="isAngularZone",Ya=zm+"_ID",uD=0,Mn=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Ht(!1);onMicrotaskEmpty=new Ht(!1);onStable=new Ht(!1);onError=new Ht(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=_x}=e;if(typeof Zone>"u")throw new Ce(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,hD(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(zm)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Ce(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Ce(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,dD,Su,Su);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},dD={};function Gm(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function fD(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Hm(()=>{n.callbackScheduled=!1,em(n),n.isCheckStableRunning=!0,Gm(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),em(n)}function hD(n){let e=()=>{fD(n)},t=uD++;n._inner=n._inner.fork({name:"angular",properties:{[zm]:!0,[Ya]:t,[Ya+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(pD(c))return i.invokeTask(s,o,a,c);try{return C0(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),D0(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return C0(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!mD(c)&&e(),D0(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,em(n),Gm(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function em(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function C0(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function D0(n){n._nesting--,Gm(n)}var Za=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Ht;onMicrotaskEmpty=new Ht;onStable=new Ht;onError=new Ht;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function pD(n){return bx(n,"__ignore_ng_zone__")}function mD(n){return bx(n,"__scheduler_tick__")}function bx(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var rr=class{_console=console;handleError(e){this._console.error("ERROR",e)}},Ri=new Te("",{factory:()=>{let n=ie(Mn),e=ie(zt),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(rr),t.handleError(i))})}}}),Ex={provide:Es,useValue:()=>{let n=ie(rr,{optional:!0})},multi:!0},gD=new Te("",{factory:()=>{let n=ie(Gt).defaultView;if(!n)return;let e=ie(Ri),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),ie(cr).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function jm(){return So([U0(()=>{ie(gD)})])}function Ni(n,e){let[t,i,r]=Dp(n,e?.equal),s=t,o=s[_n];return s.set=i,s.update=r,s.asReadonly=Sx.bind(s),s}function Sx(){let n=this[_n];if(n.readonlyFn===void 0){let e=()=>this();e[_n]=n,n.readonlyFn=e}return n.readonlyFn}var bo=class{},uc=new Te("",{factory:()=>!0});var Wm=new Te("");var $m=(()=>{class n{static \u0275prov=Oe({token:n,providedIn:"root",factory:()=>new tm})}return n})(),tm=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},nm=class{[_n];constructor(e){this[_n]=e}destroy(){this[_n].destroy()}};function hn(n){return Tp(n)}function yc(n){return{toString:n}.toString()}function DD(n){return typeof n=="function"}function tM(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var qu=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}},Fo=(()=>{let n=()=>nM;return n.ngInherit=!0,n})();function nM(n){return n.type.prototype.ngOnChanges&&(n.setInput=AD),TD}function TD(){let n=rM(this),e=n?.current;if(e){let t=n.previous;if(t===Rr)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function AD(n,e,t,i,r){let s=this.declaredInputs[i],o=rM(n)||ID(n,{previous:Rr,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new qu(l&&l.currentValue,t,c===Rr),tM(n,e,r,t)}var iM="__ngSimpleChanges__";function rM(n){return n[iM]||null}function ID(n,e){return n[iM]=e}var wx=[];var _t=function(n,e=null,t){for(let i=0;i<wx.length;i++){let r=wx[i];r(n,e,t)}},ot=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(ot||{});function RD(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=nM(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function sM(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function Gu(n,e,t){oM(n,e,3,t)}function ju(n,e,t,i){(n[Ue]&3)===t&&oM(n,e,t,i)}function qm(n,e){let t=n[Ue];(t&3)===e&&(t&=16383,t+=1,n[Ue]=t)}function oM(n,e,t,i){let r=i!==void 0?n[ws]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[ws]+=65536),(a<s||s==-1)&&(ND(n,t,e,c),n[ws]=(n[ws]&4294901760)+c+2),c++}function Cx(n,e){_t(ot.LifecycleHookStart,n,e);let t=je(null);try{e.call(n)}finally{je(t),_t(ot.LifecycleHookEnd,n,e)}}function ND(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Ue]>>14<n[ws]>>16&&(n[Ue]&3)===e&&(n[Ue]+=16384,Cx(a,s)):Cx(a,s)}var Io=-1,Is=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function PD(n){return(n.flags&8)!==0}function OD(n){return(n.flags&16)!==0}function LD(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];kD(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function FD(n){return n===3||n===4||n===6}function kD(n){return n.charCodeAt(0)===64}function hc(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?Dx(n,t,r,null,e[++i]):Dx(n,t,r,null,null))}}return n}function Dx(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function aM(n){return n!==Io}function Xu(n){return n&32767}function UD(n){return n>>16}function Yu(n,e){let t=UD(n),i=e;for(;t>0;)i=i[Ss],t--;return i}var ig=!0;function Tx(n){let e=ig;return ig=n,e}var BD=256,cM=BD-1,lM=5,VD=0,Pi={};function HD(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(bs)&&(i=t[bs]),i==null&&(i=t[bs]=VD++);let r=i&cM,s=1<<r;e.data[n+(r>>lM)]|=s}function Zu(n,e){let t=uM(n,e);if(t!==-1)return t;let i=e[Fe];i.firstCreatePass&&(n.injectorIndex=e.length,Xm(i.data,n),Xm(e,null),Xm(i.blueprint,null));let r=wg(n,e),s=n.injectorIndex;if(aM(r)){let o=Xu(r),a=Yu(r,e),c=a[Fe].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Xm(n,e){n.push(0,0,0,0,0,0,0,0,e)}function uM(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function wg(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=mM(r),i===null)return Io;if(t++,r=r[Ss],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Io}function rg(n,e,t){HD(n,e,t)}function dM(n,e,t){if(t&8||n!==void 0)return n;Du(e,"NodeInjector")}function fM(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[Nr],s=Nn(void 0);try{return r?r.get(e,i,t&8):mm(e,i,t&8)}finally{Nn(s)}}return dM(i,e,t)}function hM(n,e,t,i=0,r){if(n!==null){if(e[Ue]&2048&&!(i&2)){let o=WD(n,e,t,i,Pi);if(o!==Pi)return o}let s=pM(n,e,t,i,Pi);if(s!==Pi)return s}return fM(e,t,i,r)}function pM(n,e,t,i,r){let s=GD(t);if(typeof s=="function"){if(!km(e,n,i))return i&1?dM(r,t,i):fM(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))Du(t);else return o}finally{Um()}}else if(typeof s=="number"){let o=null,a=uM(n,e),c=Io,l=i&1?e[Yn][Xn]:null;for((a===-1||i&4)&&(c=a===-1?wg(n,e):e[a+8],c===Io||!Ix(i,!1)?a=-1:(o=e[Fe],a=Xu(c),e=Yu(c,e)));a!==-1;){let u=e[Fe];if(Ax(s,a,u.data)){let d=zD(a,e,t,o,i,l);if(d!==Pi)return d}c=e[a+8],c!==Io&&Ix(i,e[Fe].data[a+8]===l)&&Ax(s,a,e)?(o=u,a=Xu(c),e=Yu(c,e)):a=-1}}return r}function zD(n,e,t,i,r,s){let o=e[Fe],a=o.data[n+8],c=i==null?Fr(a)&&ig:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=Wu(a,o,t,c,l);return u!==null?pc(e,o,u,a,r):Pi}function Wu(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,d=i?a:a+u,f=r?a+u:l;for(let h=d;h<f;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&Di(h)&&h.type===t)return c}return null}function pc(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof Is){let a=s;if(a.resolving)throw pm("");let c=Tx(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,d=a.injectImpl?Nn(a.injectImpl):null,f=km(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&RD(t,o[t],e)}finally{d!==null&&Nn(d),Tx(c),a.resolving=!1,Um()}}return s}function GD(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(bs)?n[bs]:void 0;return typeof e=="number"?e>=0?e&cM:jD:e}function Ax(n,e,t){let i=1<<n;return!!(t[e+(n>>lM)]&i)}function Ix(n,e){return!(n&2)&&!(n&1&&e)}var As=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return hM(this._tNode,this._lView,e,ys(i),t)}};function jD(){return new As(Fn(),yt())}function Vr(n){return yc(()=>{let e=n.prototype.constructor,t=e[qa]||sg(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[qa]||sg(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function sg(n){return sm(n)?()=>{let e=sg(on(n));return e&&e()}:_s(n)}function WD(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Ue]&2048&&!Do(o);){let a=pM(s,o,t,i|2,Pi);if(a!==Pi)return a;let c=s.parent;if(!c){let l=o[Em];if(l){let u=l.get(t,Pi,i);if(u!==Pi)return u}c=mM(o),o=o[Ss]}s=c}return r}function mM(n){let e=n[Fe],t=e.type;return t===2?e.declTNode:t===1?n[Xn]:null}function $D(){return ko(Fn(),yt())}function ko(n,e){return new ur(Ti(n,e))}var ur=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=$D}return n})();function qD(n){return n instanceof ur?n.nativeElement:n}function XD(){return this._results[Symbol.iterator]()}var Ku=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Vt}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=L0(e);(this._changesDetected=!O0(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=XD};function gM(n){return(n.flags&128)===128}var Cg=(function(n){return n[n.OnPush=0]="OnPush",n[n.Default=1]="Default",n})(Cg||{}),vM=new Map,YD=0;function ZD(){return YD++}function KD(n){vM.set(n[Or],n)}function og(n){vM.delete(n[Or])}var Rx="__ngContext__";function Ro(n,e){Ci(e)?(n[Rx]=e[Or],KD(e)):n[Rx]=e}function yM(n){return xM(n[Co])}function _M(n){return xM(n[qn])}function xM(n){for(;n!==null&&!li(n);)n=n[qn];return n}var JD;function Dg(n){JD=n}var sd=new Te("",{factory:()=>QD}),QD="ng";var od=new Te(""),_c=new Te("",{providedIn:"platform",factory:()=>"unknown"});var ad=new Te("",{factory:()=>ie(Gt).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var MM=!1,bM=new Te("",{factory:()=>MM});var eT=(n,e,t,i)=>{};function tT(n,e,t,i){eT(n,e,t,i)}function Tg(n){return(n.flags&32)===32}var nT=()=>null;function EM(n,e,t=!1){return nT(n,e,t)}function SM(n,e){let t=n.contentQueries;if(t!==null){let i=je(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];ku(s),a.contentQueries(2,e[o],o)}}}finally{je(i)}}}function ag(n,e,t){ku(0);let i=je(null);try{e(n,t)}finally{je(i)}}function wM(n,e,t){if(Sm(e)){let i=je(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{je(i)}}}var di=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(di||{});var cg=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${im})`}};function CM(n){return n instanceof cg?n.changingThisBreaksApplicationSecurity:n}function iT(n,e){return n.createText(e)}function rT(n,e,t){n.setValue(e,t)}function DM(n,e,t){return n.createElement(e,t)}function Ju(n,e,t,i,r){n.insertBefore(e,t,i,r)}function TM(n,e,t){n.appendChild(e,t)}function Nx(n,e,t,i,r){i!==null?Ju(n,e,t,i,r):TM(n,e,t)}function sT(n,e,t,i){n.removeChild(null,e,t,i)}function oT(n,e,t){n.setAttribute(e,"style",t)}function aT(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function AM(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&LD(n,e,i),r!==null&&aT(n,e,r),s!==null&&oT(n,e,s)}function IM(n){return n instanceof Function?n():n}function cT(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var RM="ng-template";function lT(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&cT(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Ag(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function Ag(n){return n.type===4&&n.value!==RM}function uT(n,e,t){let i=n.type===4&&!t?RM:n.value;return e===i}function dT(n,e,t){let i=4,r=n.attrs,s=r!==null?pT(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!ui(i)&&!ui(c))return!1;if(o&&ui(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!uT(n,c,t)||c===""&&e.length===1){if(ui(i))return!1;o=!0}}else if(i&8){if(r===null||!lT(n,r,c,t)){if(ui(i))return!1;o=!0}}else{let l=e[++a],u=fT(c,r,Ag(n),t);if(u===-1){if(ui(i))return!1;o=!0;continue}if(l!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(ui(i))return!1;o=!0}}}}return ui(i)||o}function ui(n){return(n&1)===0}function fT(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return mT(e,n)}function hT(n,e,t=!1){for(let i=0;i<e.length;i++)if(dT(n,e[i],t))return!0;return!1}function pT(n){for(let e=0;e<n.length;e++){let t=n[e];if(FD(t))return e}return n.length}function mT(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function Px(n,e){return n?":not("+e.trim()+")":e}function gT(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!ui(o)&&(e+=Px(s,r),r=""),i=o,s=s||!ui(i);t++}return r!==""&&(e+=Px(s,r)),e}function vT(n){return n.map(gT).join(",")}function yT(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!ui(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var dr={};function Ig(n,e,t,i,r,s,o,a,c,l,u){let d=Ln+i,f=d+r,h=_T(d,f),g=typeof l=="function"?l():l;return h[Fe]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function _T(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:dr);return t}function xT(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Ig(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Rg(n,e,t,i,r,s,o,a,c,l,u){let d=e.blueprint.slice();return d[ci]=r,d[Ue]=i|4|128|8|64|1024,(l!==null||n&&n[Ue]&2048)&&(d[Ue]|=2048),Cm(d),d[Zt]=d[Ss]=n,d[fn]=t,d[ar]=o||n&&n[ar],d[Lt]=a||n&&n[Lt],d[Nr]=c||n&&n[Nr]||null,d[Xn]=s,d[Or]=ZD(),d[nc]=u,d[Em]=l,d[Yn]=e.type==2?n[Yn]:d,d}function MT(n,e,t){let i=Ti(e,n),r=xT(t),s=n[ar].rendererFactory,o=Ng(n,Rg(n,r,null,NM(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function NM(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function PM(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Ng(n,e){return n[Co]?n[bm][qn]=e:n[Co]=e,n[bm]=e,e}function fr(n=1){OM(Tn(),yt(),Ts()+n,!1)}function OM(n,e,t,i){if(!i)if((e[Ue]&3)===3){let s=n.preOrderCheckHooks;s!==null&&Gu(e,s,t)}else{let s=n.preOrderHooks;s!==null&&ju(e,s,0,t)}kr(t)}var cd=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(cd||{});function lg(n,e,t,i){let r=je(null);try{let[s,o,a]=n.inputs[t],c=null;(o&cd.SignalBased)!==0&&(c=e[s][_n]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):tM(e,c,s,i)}finally{je(r)}}var lr=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(lr||{}),bT;function Pg(n,e){return bT(n,e)}var No=new Set,Og=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(Og||{}),xc=new Te(""),Ox=new Set;function Lg(n){Ox.has(n)||(Ox.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var LM=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Oe({token:n,providedIn:"root",factory:()=>new n})}return n})();var FM=new Te("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:ie(zt)})});function kM(n,e,t){let i=n.get(FM);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function ET(n,e){for(let[t,i]of e)kM(n,i.animateFns)}function ST(n,e){let t=n.get(FM);if(Array.isArray(e))for(let i of e)t.queue.delete(i);else t.queue.delete(e)}function Lx(n,e,t,i){let r=n?.[rc]?.enter;e!==null&&r&&r.has(t.index)&&ET(i,r)}function Ao(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;li(r)?c=r:Ci(r)&&(l=!0,r=r[ci]);let u=Zn(r);n===0&&i!==null?(Lx(a,i,s,t),o==null?TM(e,i,u):Ju(e,i,u,o||null,!0)):n===1&&i!==null?(Lx(a,i,s,t),Ju(e,i,u,o||null,!0)):n===2?Fx(a,s,t,d=>{sT(e,u,l,d)}):n===3&&Fx(a,s,t,()=>{e.destroyNode(u)}),c!=null&&kT(e,n,t,c,s,i,o)}}function wT(n,e){UM(n,e),e[ci]=null,e[Xn]=null}function CT(n,e,t,i,r,s){i[ci]=r,i[Xn]=e,ld(n,i,t,1,r,s)}function UM(n,e){e[ar].changeDetectionScheduler?.notify(9),ld(n,e,e[Lt],2,null,null)}function DT(n){let e=n[Co];if(!e)return Ym(n[Fe],n);for(;e;){let t=null;if(Ci(e))t=e[Co];else{let i=e[bn];i&&(t=i)}if(!t){for(;e&&!e[qn]&&e!==n;)Ci(e)&&Ym(e[Fe],e),e=e[Zt];e===null&&(e=n),Ci(e)&&Ym(e[Fe],e),t=e&&e[qn]}e=t}}function Fg(n,e){let t=n[Cs],i=t.indexOf(e);t.splice(i,1)}function BM(n,e){if(Ds(e))return;let t=e[Lt];t.destroyNode&&ld(n,e,t,3,null,null),DT(e)}function Ym(n,e){if(Ds(e))return;let t=je(null);try{e[Ue]&=-129,e[Ue]|=256,e[On]&&Ba(e[On]),IT(n,e),AT(n,e),e[Fe].type===1&&e[Lt].destroy();let i=e[Pr];if(i!==null&&li(e[Zt])){i!==e[Zt]&&Fg(i,e);let r=e[wi];r!==null&&r.detachView(n)}og(e)}finally{je(t)}}function Fx(n,e,t,i){let r=n?.[rc];if(r?.enter?.has(e.index)&&ST(t,r.enter.get(e.index).animateFns),r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&No.add(n[Or]),kM(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),TT(n,i)}else n&&No.delete(n[Or]),i(!1)},r)}function TT(n,e){let t=n[rc]?.running;if(t){t.then(()=>{n[rc].running=void 0,No.delete(n[Or]),e(!0)});return}e(!1)}function AT(n,e){let t=n.cleanup,i=e[wo];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[wo]=null);let r=e[nr];if(r!==null){e[nr]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[ic];if(s!==null){e[ic]=null;for(let o of s)o.destroy()}}function IT(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof Is)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];_t(ot.LifecycleHookStart,a,c);try{c.call(a)}finally{_t(ot.LifecycleHookEnd,a,c)}}else{_t(ot.LifecycleHookStart,r,s);try{s.call(r)}finally{_t(ot.LifecycleHookEnd,r,s)}}}}}function RT(n,e,t){return NT(n,e.parent,t)}function NT(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[ci];if(Fr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===di.None||r===di.Emulated)return null}return Ti(i,t)}function PT(n,e,t){return LT(n,e,t)}function OT(n,e,t){return n.type&40?Ti(n,t):null}var LT=OT,kx;function kg(n,e,t,i){let r=RT(n,i,e),s=e[Lt],o=i.parent||e[Xn],a=PT(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)Nx(s,r,t[c],a,!1);else Nx(s,r,t,a,!1);kx!==void 0&&kx(s,i,e,t,r)}function dc(n,e){if(e!==null){let t=e.type;if(t&3)return Ti(e,n);if(t&4)return ug(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return dc(n,i);{let r=n[e.index];return li(r)?ug(-1,r):Zn(r)}}else{if(t&128)return dc(n,e.next);if(t&32)return Pg(e,n)()||Zn(n[e.index]);{let i=VM(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Ar(n[Yn]);return dc(r,i)}else return dc(n,e.next)}}}return null}function VM(n,e){if(e!==null){let i=n[Yn][Xn],r=e.projection;return i.projection[r]}return null}function ug(n,e){let t=bn+n+1;if(t<e.length){let i=e[t],r=i[Fe].firstChild;if(r!==null)return dc(i,r)}return e[Lr]}function Ug(n,e,t,i,r,s,o){for(;t!=null;){let a=i[Nr];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&Ro(Zn(c),i),t.flags|=2),!Tg(t))if(l&8)Ug(n,e,t.child,i,r,s,!1),Ao(e,n,a,r,c,t,s,i);else if(l&32){let u=Pg(t,i),d;for(;d=u();)Ao(e,n,a,r,d,t,s,i);Ao(e,n,a,r,c,t,s,i)}else l&16?FT(n,e,i,t,r,s):Ao(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function ld(n,e,t,i,r,s){Ug(t,i,n.firstChild,e,r,s,!1)}function FT(n,e,t,i,r,s){let o=t[Yn],c=o[Xn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];Ao(e,n,t[Nr],r,u,i,s,t)}else{let l=c,u=o[Zt];gM(i)&&(l.flags|=128),Ug(n,e,l,u,r,s,!0)}}function kT(n,e,t,i,r,s,o){let a=i[Lr],c=Zn(i);a!==c&&Ao(e,n,t,s,a,r,o);for(let l=bn;l<i.length;l++){let u=i[l];ld(u[Fe],u,n,e,s,a)}}function UT(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:lr.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=lr.Important),n.setStyle(t,i,r,s))}}function HM(n,e,t,i,r){let s=Ts(),o=i&2;try{kr(-1),o&&e.length>Ln&&OM(n,e,Ln,!1);let a=o?ot.TemplateUpdateStart:ot.TemplateCreateStart;_t(a,r,t),t(i,r)}finally{kr(s);let a=o?ot.TemplateUpdateEnd:ot.TemplateCreateEnd;_t(a,r,t)}}function Bg(n,e,t){WT(n,e,t),(t.flags&64)===64&&$T(n,e,t)}function zM(n,e,t=Ti){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function BT(n,e,t,i){let s=i.get(bM,MM)||t===di.ShadowDom||t===di.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return VT(o),o}function VT(n){HT(n)}var HT=()=>null;function zT(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function GM(n,e,t,i,r,s){let o=e[Fe];if(Vg(n,o,e,t,i)){Fr(n)&&jT(e,n.index);return}n.type&3&&(t=zT(t)),GT(n,e,t,i,r,s)}function GT(n,e,t,i,r,s){if(n.type&3){let o=Ti(n,e);i=s!=null?s(i,n.value||"",t):i,r.setProperty(o,t,i)}else n.type&12}function jT(n,e){let t=Kn(e,n);t[Ue]&16||(t[Ue]|=64)}function WT(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Fr(t)&&MT(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Zu(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=pc(e,n,o,t);if(Ro(c,e),s!==null&&XT(e,o-i,c,a,t,s),Di(a)){let l=Kn(t.index,e);l[fn]=pc(e,n,o,t)}}}function $T(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=dx();try{kr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Fu(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&qT(c,l)}}finally{kr(-1),Fu(o)}}function qT(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function jM(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];hT(e,s.selectors,!1)&&(i??=[],Di(s)?i.unshift(s):i.push(s))}return i}function XT(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];lg(i,t,c,l)}}function YT(n,e,t,i,r){let s=Ln+t,o=e[Fe],a=r(o,e,n,i,t);e[s]=a,To(n,!0);let c=n.type===2;return c?(AM(e[Lt],a,n),(J0()===0||oc(n))&&Ro(a,e),Q0()):Ro(a,e),Vu()&&(!c||!Tg(n))&&kg(o,e,a,n),n}function ZT(n){let e=n;return Pm()?sx():(e=e.parent,To(e,!1)),e}function KT(n,e){let t=n[Nr];if(!t)return;let i;try{i=t.get(Ri,null)}catch{i=null}i?.(e)}function Vg(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],d=e.data[l];lg(d,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];lg(u,l,i,r),a=!0}return a}function JT(n,e){let t=Kn(e,n),i=t[Fe];QT(i,t);let r=t[ci];r!==null&&t[nc]===null&&(t[nc]=EM(r,t[Nr])),_t(ot.ComponentStart);try{Hg(i,t,t[fn])}finally{_t(ot.ComponentEnd,t[fn])}}function QT(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Hg(n,e,t){Uu(e);try{let i=n.viewQuery;i!==null&&ag(1,i,t);let r=n.template;r!==null&&HM(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[wi]?.finishViewCreation(n),n.staticContentQueries&&SM(n,e),n.staticViewQueries&&ag(2,n.viewQuery,t);let s=n.components;s!==null&&eA(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Ue]&=-5,Bu()}}function eA(n,e){for(let t=0;t<e.length;t++)JT(n,e[t])}function tA(n,e,t,i){let r=je(null);try{let s=e.tView,a=n[Ue]&4096?4096:16,c=Rg(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[Pr]=l;let u=n[wi];return u!==null&&(c[wi]=u.createEmbeddedView(s)),Hg(s,c,t),c}finally{je(r)}}function Ux(n,e){return!e||e.firstChild===null||gM(n)}function mc(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(Zn(s)),li(s)&&WM(s,i);let o=t.type;if(o&8)mc(n,e,t.child,i);else if(o&32){let a=Pg(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=VM(e,t);if(Array.isArray(a))i.push(...a);else{let c=Ar(e[Yn]);mc(c[Fe],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function WM(n,e){for(let t=bn;t<n.length;t++){let i=n[t],r=i[Fe].firstChild;r!==null&&mc(i[Fe],i,r,e)}n[Lr]!==n[ci]&&e.push(n[Lr])}function $M(n){if(n[Ru]!==null){for(let e of n[Ru])e.impl.addSequence(e);n[Ru].length=0}}var qM=[];function nA(n){return n[On]??iA(n)}function iA(n){let e=qM.pop()??Object.create(sA);return e.lView=n,e}function rA(n){n.lView[On]!==n&&(n.lView=null,qM.push(n))}var sA=et(ae({},Fa),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{lc(n.lView)},consumerOnSignalRead(){this.lView[On]=this}});function oA(n){let e=n[On]??Object.create(aA);return e.lView=n,e}var aA=et(ae({},Fa),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Ar(n.lView);for(;e&&!XM(e[Fe]);)e=Ar(e);e&&Dm(e)},consumerOnSignalRead(){this.lView[On]=this}});function XM(n){return n.type!==2}function YM(n){if(n[ic]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[ic])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Ue]&8192)}}var cA=100;function ZM(n,e=0){let i=n[ar].rendererFactory,r=!1;r||i.begin?.();try{lA(n,e)}finally{r||i.end?.()}}function lA(n,e){let t=Om();try{Lm(!0),dg(n,e);let i=0;for(;cc(n);){if(i===cA)throw new Ce(103,!1);i++,dg(n,1)}}finally{Lm(t)}}function uA(n,e,t,i){if(Ds(e))return;let r=e[Ue],s=!1,o=!1;Uu(e);let a=!0,c=null,l=null;s||(XM(n)?(l=nA(e),c=Ua(l)):Wl()===null?(a=!1,l=oA(e),c=Ua(l)):e[On]&&(Ba(e[On]),e[On]=null));try{Cm(e),ax(n.bindingStartIndex),t!==null&&HM(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&Gu(e,h,null)}else{let h=n.preOrderHooks;h!==null&&ju(e,h,0,null),qm(e,0)}if(o||dA(e),YM(e),KM(e,0),n.contentQueries!==null&&SM(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&Gu(e,h)}else{let h=n.contentHooks;h!==null&&ju(e,h,1),qm(e,1)}hA(n,e);let d=n.components;d!==null&&QM(e,d,0);let f=n.viewQuery;if(f!==null&&ag(2,f,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&Gu(e,h)}else{let h=n.viewHooks;h!==null&&ju(e,h,2),qm(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Iu]){for(let h of e[Iu])h();e[Iu]=null}s||($M(e),e[Ue]&=-73)}catch(u){throw s||lc(e),u}finally{l!==null&&($l(l,c),a&&rA(l)),Bu()}}function KM(n,e){for(let t=yM(n);t!==null;t=_M(t))for(let i=bn;i<t.length;i++){let r=t[i];JM(r,e)}}function dA(n){for(let e=yM(n);e!==null;e=_M(e)){if(!(e[Ue]&2))continue;let t=e[Cs];for(let i=0;i<t.length;i++){let r=t[i];Dm(r)}}}function fA(n,e,t){_t(ot.ComponentStart);let i=Kn(e,n);try{JM(i,t)}finally{_t(ot.ComponentEnd,i[fn])}}function JM(n,e){Pu(n)&&dg(n,e)}function dg(n,e){let i=n[Fe],r=n[Ue],s=n[On],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&ql(s)),o||=!1,s&&(s.dirty=!1),n[Ue]&=-9217,o)uA(i,n,i.template,n[fn]);else if(r&8192){let a=je(null);try{YM(n),KM(n,1);let c=i.components;c!==null&&QM(n,c,1),$M(n)}finally{je(a)}}}function QM(n,e,t){for(let i=0;i<e.length;i++)fA(n,e[i],t)}function hA(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)kr(~r);else{let s=r,o=t[++i],a=t[++i];ux(o,s);let c=e[s];_t(ot.HostBindingsUpdateStart,c);try{a(2,c)}finally{_t(ot.HostBindingsUpdateEnd,c)}}}}finally{kr(-1)}}function zg(n,e){let t=Om()?64:1088;for(n[ar].changeDetectionScheduler?.notify(e);n;){n[Ue]|=t;let i=Ar(n);if(Do(n)&&!i)return n;n=i}return null}function eb(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function pA(n,e,t,i=!0){let r=e[Fe];if(mA(r,e,n,t),i){let o=ug(t,n),a=e[Lt],c=a.parentNode(n[Lr]);c!==null&&CT(r,n[Xn],a,e,c,o)}let s=e[nc];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function fg(n,e){if(n.length<=bn)return;let t=bn+e,i=n[t];if(i){let r=i[Pr];r!==null&&r!==n&&Fg(r,i),e>0&&(n[t-1][qn]=i[qn]);let s=Qa(n,bn+e);wT(i[Fe],i);let o=s[wi];o!==null&&o.detachView(s[Fe]),i[Zt]=null,i[qn]=null,i[Ue]&=-129}return i}function mA(n,e,t,i){let r=bn+i,s=t.length;i>0&&(t[r-1][qn]=e),i<s-bn?(e[qn]=t[r],gm(t,bn+i,e)):(t.push(e),e[qn]=null),e[Zt]=t;let o=e[Pr];o!==null&&t!==o&&tb(o,e);let a=e[wi];a!==null&&a.insertView(n),Ou(e),e[Ue]|=128}function tb(n,e){let t=n[Cs],i=e[Zt];if(Ci(i))n[Ue]|=2;else{let r=i[Zt][Yn];e[Yn]!==r&&(n[Ue]|=2)}t===null?n[Cs]=[e]:t.push(e)}var Br=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Fe];return mc(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[fn]}set context(e){this._lView[fn]=e}get destroyed(){return Ds(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Zt];if(li(e)){let t=e[sc],i=t?t.indexOf(this):-1;i>-1&&(fg(e,i),Qa(t,i))}this._attachedToViewContainer=!1}BM(this._lView[Fe],this._lView)}onDestroy(e){Tm(this._lView,e)}markForCheck(){zg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Ue]&=-129}reattach(){Ou(this._lView),this._lView[Ue]|=128}detectChanges(){this._lView[Ue]|=1024,ZM(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Ce(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Do(this._lView),t=this._lView[Pr];t!==null&&!e&&Fg(t,this._lView),UM(this._lView[Fe],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Ce(902,!1);this._appRef=e;let t=Do(this._lView),i=this._lView[Pr];i!==null&&!t&&tb(i,this._lView),Ou(this._lView)}};var Rs=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=gA;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=tA(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Br(s)}}return n})();function gA(){return Gg(Fn(),yt())}function Gg(n,e){return n.type&4?new Rs(e,n,ko(n,e)):null}function jg(n,e,t,i,r){let s=n.data[e];if(s===null)s=vA(n,e,t,i,r),lx()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=rx();s.injectorIndex=o===null?-1:o.injectorIndex}return To(s,!0),s}function vA(n,e,t,i,r){let s=Nm(),o=Pm(),a=o?s:s&&s.parent,c=n.data[e]=_A(n,a,t,e,i,r);return yA(n,c,s,o),c}function yA(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function _A(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return tx()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,fieldIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var xA=()=>null;function Bx(n,e){return xA(n,e)}var nb=class{},ud=class{},hg=class{resolveComponentFactory(e){throw new Ce(917,!1)}},Mc=class{static NULL=new hg},Ns=class{},bc=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>MA()}return n})();function MA(){let n=yt(),e=Fn(),t=Kn(e.index,n);return(Ci(t)?t:n)[Lt]}var ib=(()=>{class n{static \u0275prov=Oe({token:n,providedIn:"root",factory:()=>null})}return n})();var $u={},pg=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,$u,i);return r!==$u||t===$u?r:this.parentInjector.get(e,t,i)}};function Vx(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=rm(r,a);else if(s==2){let c=a,l=e[++o];i=rm(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Tt(n,e=0){let t=yt();if(t===null)return qe(n,e);let i=Fn();return hM(i,t,on(n),e)}function rb(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}SA(n,e,t,a,s,c,l)}s!==null&&i!==null&&bA(t,i,s)}function bA(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Ce(-301,!1);i.push(e[r],s)}}function EA(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function SA(n,e,t,i,r,s,o){let a=i.length,c=null;for(let f=0;f<a;f++){let h=i[f];c===null&&Di(h)&&(c=h,EA(n,t,f)),rg(Zu(t,e),n,h.type)}IA(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let f=0;f<a;f++){let h=i[f];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,d=PM(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let f=0;f<a;f++){let h=i[f];if(t.mergedAttrs=hc(t.mergedAttrs,h.hostAttrs),CA(n,t,e,d,h),AA(d,h,r),o!==null&&o.has(h)){let[_,m]=o.get(h);t.directiveToIndex.set(h.type,[d,_+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,d);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}wA(n,t,s)}function wA(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))Hx(0,e,r,i),Hx(1,e,r,i),Gx(e,i,!1);else{let s=t.get(r);zx(0,e,s,i),zx(1,e,s,i),Gx(e,i,!0)}}}function Hx(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),sb(e,s)}}function zx(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),sb(e,o)}}function sb(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function Gx(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||Ag(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function CA(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=_s(r.type,!0)),o=new Is(s,Di(r),Tt,null);n.blueprint[i]=o,t[i]=o,DA(n,e,i,PM(n,t,r.hostVars,dr),r)}function DA(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;TA(o)!=a&&o.push(a),o.push(t,i,s)}}function TA(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function AA(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Di(e)&&(t[""]=n)}}function IA(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function ob(n,e,t,i,r,s,o,a){let c=e[Fe],l=c.consts,u=ac(l,o),d=jg(c,n,t,i,u);return s&&rb(c,e,d,ac(l,a),r),d.mergedAttrs=hc(d.mergedAttrs,d.attrs),d.attrs!==null&&Vx(d,d.attrs,!1),d.mergedAttrs!==null&&Vx(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function ab(n,e){sM(n,e),Sm(e)&&n.queries.elementEnd(e)}function Wg(n){return lb(n)?Array.isArray(n)||!(n instanceof Map)&&Symbol.iterator in n:!1}function cb(n,e){if(Array.isArray(n))for(let t=0;t<n.length;t++)e(n[t]);else{let t=n[Symbol.iterator](),i;for(;!(i=t.next()).done;)e(i.value)}}function lb(n){return n!==null&&(typeof n=="function"||typeof n=="object")}function RA(n,e,t){return n[e]=t}function NA(n,e){return n[e]}function dd(n,e,t){if(t===dr)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function Zm(n,e,t){return function i(r){let s=Fr(n)?Kn(n.index,e):e;zg(s,5);let o=e[fn],a=jx(e,o,t,r),c=i.__ngNextListenerFn__;for(;c;)a=jx(e,o,c,r)&&a,c=c.__ngNextListenerFn__;return a}}function jx(n,e,t,i){let r=je(null);try{return _t(ot.OutputStart,e,t),t(i)!==!1}catch(s){return KT(n,s),!1}finally{_t(ot.OutputEnd,e,t),je(r)}}function PA(n,e,t,i,r,s,o,a){let c=oc(n),l=!1,u=null;if(!i&&c&&(u=LA(e,t,s,n.index)),u!==null){let d=u.__ngLastListenerFn__||u;d.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let d=Ti(n,t),f=i?i(d):d;tT(t,f,s,a);let h=r.listen(f,s,a);if(!OA(s)){let g=i?_=>i(Zn(_[n.index])):n.index;ub(g,e,t,s,a,h,!1)}}return l}function OA(n){return n.startsWith("animation")||n.startsWith("transition")}function LA(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[wo],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function ub(n,e,t,i,r,s,o){let a=e.firstCreatePass?Im(e):null,c=Am(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}function Wx(n,e,t,i,r,s){let o=e[t],a=e[Fe],l=a.data[t].outputs[i],d=o[l].subscribe(s);ub(n.index,a,e,r,s,d,!0)}var mg=Symbol("BINDING");var Qu=class extends Mc{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Ir(e);return new Po(t,this.ngModule)}};function FA(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&cd.SignalBased)!==0};return r&&(s.transform=r),s})}function kA(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function UA(n,e,t){let i=e instanceof zt?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new pg(t,i):t}function BA(n){let e=n.get(Ns,null);if(e===null)throw new Ce(407,!1);let t=n.get(ib,null),i=n.get(bo,null);return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1}}function VA(n,e){let t=db(n);return DM(e,t,t==="svg"?W0:t==="math"?$0:null)}function db(n){return(n.selectors[0][0]||"div").toLowerCase()}var Po=class extends ud{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=FA(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=kA(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=vT(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){_t(ot.DynamicComponentStart);let a=je(null);try{let c=this.componentDef,l=HA(i,c,o,s),u=UA(c,r||this.ngModule,e),d=BA(u),f=d.rendererFactory.createRenderer(null,c),h=i?BT(f,i,c.encapsulation,u):VA(c,f),g=o?.some($x)||s?.some(p=>typeof p!="function"&&p.bindings.some($x)),_=Rg(null,l,null,512|NM(c),null,null,d,f,u,null,EM(h,u,!0));_[Ln]=h,Uu(_);let m=null;try{let p=ob(Ln,_,2,"#host",()=>l.directiveRegistry,!0,0);AM(f,h,p),Ro(h,_),Bg(l,_,p),wM(l,p,_),ab(l,p),t!==void 0&&GA(p,this.ngContentSelectors,t),m=Kn(p.index,_),_[fn]=m[fn],Hg(l,_,null)}catch(p){throw m!==null&&og(m),og(_),p}finally{_t(ot.DynamicComponentEnd),Bu()}return new ed(this.componentType,_,!!g)}finally{je(a)}}};function HA(n,e,t,i){let r=n?["ng-version","21.1.5"]:yT(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[mg].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let d=i[u];if(typeof d!="function")for(let f of d.bindings){a+=f[mg].requiredVars;let h=u+1;f.create&&(f.targetIdx=h,(s??=[]).push(f)),f.update&&(f.targetIdx=h,(o??=[]).push(f))}}let c=[e];if(i)for(let u of i){let d=typeof u=="function"?u:u.type,f=hm(d);c.push(f)}return Ig(0,null,zA(s,o),1,a,c,null,null,null,[r],null)}function zA(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function $x(n){let e=n[mg].kind;return e==="input"||e==="twoWay"}var ed=class extends nb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Nu(t[Fe],Ln),this.location=ko(this._tNode,t),this.instance=Kn(this._tNode.index,t)[fn],this.hostView=this.changeDetectorRef=new Br(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Vg(i,r[Fe],r,e,t);this.previousInputValues.set(e,t);let o=Kn(i.index,r);zg(o,1)}get injector(){return new As(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function GA(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var Hr=(()=>{class n{static __NG_ELEMENT_ID__=jA}return n})();function jA(){let n=Fn();return hb(n,yt())}var WA=Hr,fb=class extends WA{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return ko(this._hostTNode,this._hostLView)}get injector(){return new As(this._hostTNode,this._hostLView)}get parentInjector(){let e=wg(this._hostTNode,this._hostLView);if(aM(e)){let t=Yu(e,this._hostLView),i=Xu(e),r=t[Fe].data[i+8];return new As(r,t)}else return new As(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=qx(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-bn}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=Bx(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,Ux(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!DD(e),l;if(c)l=t;else{let m=t||{};l=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef,o=m.directives,a=m.bindings}let u=c?e:new Po(Ir(e)),d=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?d:this.parentInjector).get(zt,null);p&&(s=p)}let f=Ir(u.componentType??{}),h=Bx(this._lContainer,f?.id??null),g=h?.firstChild??null,_=u.create(d,r,g,s,o,a);return this.insertImpl(_.hostView,l,Ux(this._hostTNode,h)),_}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(X0(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[Zt],l=new fb(c,c[Xn],c[Zt]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return pA(o,r,s,i),e.attachToViewContainerRef(),gm(Km(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=qx(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=fg(this._lContainer,t);i&&(Qa(Km(this._lContainer),t),BM(i[Fe],i))}detach(e){let t=this._adjustIndex(e,-1),i=fg(this._lContainer,t);return i&&Qa(Km(this._lContainer),t)!=null?new Br(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function qx(n){return n[sc]}function Km(n){return n[sc]||(n[sc]=[])}function hb(n,e){let t,i=e[n.index];return li(i)?t=i:(t=eb(i,e,null,n),e[n.index]=t,Ng(e,t)),qA(t,e,n,i),new fb(t,n,e)}function $A(n,e){let t=n[Lt],i=t.createComment(""),r=Ti(e,n),s=t.parentNode(r);return Ju(t,s,i,t.nextSibling(r),!1),i}var qA=ZA,XA=()=>!1;function YA(n,e,t){return XA(n,e,t)}function ZA(n,e,t,i){if(n[Lr])return;let r;t.type&8?r=Zn(i):r=$A(e,t),n[Lr]=r}var gg=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},vg=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)$g(e,t).matches!==null&&this.queries[t].setDirty()}},yg=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=rI(e):this.predicate=e}},_g=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},xg=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,KA(t,s)),this.matchTNodeWithReadOption(e,t,Wu(t,e,s,!1,!1))}else i===Rs?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Wu(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===ur||r===Hr||r===Rs&&t.type&4)this.addMatch(t.index,-2);else{let s=Wu(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function KA(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function JA(n,e){return n.type&11?ko(n,e):n.type&4?Gg(n,e):null}function QA(n,e,t,i){return t===-1?JA(e,n):t===-2?eI(n,e,i):pc(n,n[Fe],t,e)}function eI(n,e,t){if(t===ur)return ko(e,n);if(t===Rs)return Gg(e,n);if(t===Hr)return hb(e,n)}function pb(n,e,t,i){let r=e[wi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(QA(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function Mg(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=pb(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let d=bn;d<u.length;d++){let f=u[d];f[Pr]===f[Zt]&&Mg(f[Fe],f,l,i)}if(u[Cs]!==null){let d=u[Cs];for(let f=0;f<d.length;f++){let h=d[f];Mg(h[Fe],h,l,i)}}}}}return i}function tI(n,e){return n[wi].queries[e].queryList}function nI(n,e,t){let i=new Ku((t&4)===4);return K0(n,e,i,i.destroy),(e[wi]??=new vg).queries.push(new gg(i))-1}function iI(n,e,t){let i=Tn();return i.firstCreatePass&&(sI(i,new yg(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),nI(i,yt(),e)}function rI(n){return n.split(",").map(e=>e.trim())}function sI(n,e,t){n.queries===null&&(n.queries=new _g),n.queries.track(new xg(e,t))}function $g(n,e){return n.queries.getByIndex(e)}function oI(n,e){let t=n[Fe],i=$g(t,e);return i.crossesNgTemplate?Mg(t,n,e,[]):pb(t,n,i,e)}var Oo=class{},fd=class{};var td=class extends Oo{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Qu(this);constructor(e,t,i,r=!0){super(),this.ngModuleType=e,this._parent=t;let s=fm(e);this._bootstrapComponents=IM(s.bootstrap),this._r3Injector=Vm(e,t,[{provide:Oo,useValue:this},{provide:Mc,useValue:this.componentFactoryResolver},...i],ir(e),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let e=this._r3Injector;!e.destroyed&&e.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(e){this.destroyCbs.push(e)}},nd=class extends fd{moduleType;constructor(e){super(),this.moduleType=e}create(e){return new td(this.moduleType,e,[])}};var gc=class extends Oo{injector;componentFactoryResolver=new Qu(this);instance=null;constructor(e){super();let t=new Ms([...e.providers,{provide:Oo,useValue:this},{provide:Mc,useValue:this.componentFactoryResolver}],e.parent||tc(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function Ec(n,e,t=null){return new gc({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var aI=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=_m(!1,t.type),r=i.length>0?Ec([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Oe({token:n,providedIn:"environment",factory:()=>new n(qe(zt))})}return n})();function Sc(n){return yc(()=>{let e=mb(n),t=et(ae({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===Cg.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(aI).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||di.Emulated,styles:n.styles||Pn,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&Lg("NgStandalone"),gb(t);let i=n.dependencies;return t.directiveDefs=Xx(i,cI),t.pipeDefs=Xx(i,A0),t.id=dI(t),t})}function cI(n){return Ir(n)||hm(n)}function zr(n){return yc(()=>({type:n.type,bootstrap:n.bootstrap||Pn,declarations:n.declarations||Pn,imports:n.imports||Pn,exports:n.exports||Pn,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function lI(n,e){if(n==null)return Rr;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=cd.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function uI(n){if(n==null)return Rr;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function kn(n){return yc(()=>{let e=mb(n);return gb(e),e})}function mb(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||Rr,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Pn,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,inputs:lI(n.inputs,e),outputs:uI(n.outputs),debugInfo:null}}function gb(n){n.features?.forEach(e=>e(n))}function Xx(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function dI(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function fI(n){return Object.getPrototypeOf(n.prototype).constructor}function hr(n){let e=fI(n.type),t=!0,i=[n];for(;e;){let r;if(Di(n))r=e.\u0275cmp||e.\u0275dir;else{if(e.\u0275cmp)throw new Ce(903,!1);r=e.\u0275dir}if(r){if(t){i.push(r);let o=n;o.inputs=Jm(n.inputs),o.declaredInputs=Jm(n.declaredInputs),o.outputs=Jm(n.outputs);let a=r.hostBindings;a&&vI(n,a);let c=r.viewQuery,l=r.contentQueries;if(c&&mI(n,c),l&&gI(n,l),hI(n,r),T0(n.outputs,r.outputs),Di(r)&&r.data.animation){let u=n.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let o=0;o<s.length;o++){let a=s[o];a&&a.ngInherit&&a(n),a===hr&&(t=!1)}}e=Object.getPrototypeOf(e)}pI(i)}function hI(n,e){for(let t in e.inputs){if(!e.inputs.hasOwnProperty(t)||n.inputs.hasOwnProperty(t))continue;let i=e.inputs[t];i!==void 0&&(n.inputs[t]=i,n.declaredInputs[t]=e.declaredInputs[t])}}function pI(n){let e=0,t=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=e+=r.hostVars,r.hostAttrs=hc(r.hostAttrs,t=hc(t,r.hostAttrs))}}function Jm(n){return n===Rr?{}:n===Pn?[]:n}function mI(n,e){let t=n.viewQuery;t?n.viewQuery=(i,r)=>{e(i,r),t(i,r)}:n.viewQuery=e}function gI(n,e){let t=n.contentQueries;t?n.contentQueries=(i,r,s)=>{e(i,r,s),t(i,r,s)}:n.contentQueries=e}function vI(n,e){let t=n.hostBindings;t?n.hostBindings=(i,r)=>{e(i,r),t(i,r)}:n.hostBindings=e}function yI(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=hc(n.mergedAttrs,n.attrs);let u=n.tView=Ig(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),To(n,!1);let c=xI(t,e,n,i);Vu()&&kg(t,e,c,n),Ro(c,e);let l=eb(c,e,c,n);e[i+Ln]=l,Ng(e,l),YA(l,n,e)}function _I(n,e,t,i,r,s,o,a,c,l,u){let d=t+Ln,f;return e.firstCreatePass?(f=jg(e,d,4,o||null,a||null),Rm()&&rb(e,n,f,ac(e.consts,l),jM),sM(e,f)):f=e.data[d],yI(f,n,e,t,i,r,s,c),oc(f)&&Bg(e,n,f),l!=null&&zM(n,f,u),f}function hd(n,e,t,i,r,s,o,a){let c=yt(),l=Tn(),u=ac(l.consts,s);return _I(c,l,n,e,t,i,r,u,void 0,o,a),hd}var xI=MI;function MI(n,e,t,i){return Hu(!0),e[Lt].createComment("")}var qg=(()=>{class n{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})();function vb(n){return typeof n=="function"&&n[_n]!==void 0}function Xg(n){return vb(n)&&typeof n.set=="function"}var Yg=new Te("");function Os(n){return!!n&&typeof n.then=="function"}function yb(n){return!!n&&typeof n.subscribe=="function"}var _b=new Te("");var Zg=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=ie(_b,{optional:!0})??[];injector=ie(ai);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=dn(this.injector,r);if(Os(s))t.push(s);else if(yb(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),pd=new Te("");function xb(){Cp(()=>{let n="";throw new Ce(600,n)})}function Mb(n){return n.isBoundToModule}var bI=10;var Ls=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=ie(Ri);afterRenderManager=ie(LM);zonelessEnabled=ie(uc);rootEffectScheduler=ie($m);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Vt;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=ie(Ur);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Dt(t=>!t))}constructor(){ie(xc,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=ie(zt);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=ai.NULL){return this._injector.get(Mn).run(()=>{_t(ot.BootstrapComponentStart);let o=t instanceof ud;if(!this._injector.get(Zg).done){let g="";throw new Ce(405,g)}let c;o?c=t:c=this._injector.get(Mc).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=Mb(c)?void 0:this._injector.get(Oo),u=i||c.selector,d=c.create(r,[],u,l),f=d.location.nativeElement,h=d.injector.get(Yg,null);return h?.registerApplication(f),d.onDestroy(()=>{this.detachView(d.hostView),fc(this.components,d),h?.unregisterApplication(f)}),this._loadComponent(d),_t(ot.BootstrapComponentEnd,d),d})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){_t(ot.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Og.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw _t(ot.ChangeDetectionEnd),new Ce(101,!1);let t=je(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,je(t),this.afterTick.next(),_t(ot.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ns,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<bI;){_t(ot.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{_t(ot.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!cc(r))continue;let s=i&&!this.zonelessEnabled?0:1;ZM(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>cc(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;fc(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(pd,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>fc(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Ce(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function fc(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}var n4=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";function md(n,e,t){let i=yt(),r=Lu();if(dd(i,r,e)){let s=Tn(),o=Bm();GM(o,i,n,e,i[Lt],t)}return md}function Yx(n,e,t,i,r){Vg(e,n,t,r?"class":"style",i)}function Kt(n,e,t,i){let r=yt(),s=r[Fe],o=n+Ln,a=s.firstCreatePass?ob(o,r,2,e,jM,Rm(),t,i):s.data[o];if(YT(a,r,n,e,EI),oc(a)){let c=r[Fe];Bg(c,r,a),wM(c,a,r)}return i!=null&&zM(r,a),Kt}function Jt(){let n=Tn(),e=Fn(),t=ZT(e);return n.firstCreatePass&&ab(n,t),nx(t)&&ix(),ex(),t.classesWithoutHost!=null&&PD(t)&&Yx(n,t,yt(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&OD(t)&&Yx(n,t,yt(),t.stylesWithoutHost,!1),Jt}function Uo(n,e,t,i){return Kt(n,e,t,i),Jt(),Uo}var EI=(n,e,t,i,r)=>(Hu(!0),DM(e[Lt],i,vx()));function gd(){return yt()}var wc="en-US";var SI=wc;function bb(n){typeof n=="string"&&(SI=n.toLowerCase().replace(/_/g,"-"))}function Oi(n,e,t){let i=yt(),r=Tn(),s=Fn();return Eb(r,i,i[Lt],s,n,e,t),Oi}function Eb(n,e,t,i,r,s,o){let a=!0,c=null;if((i.type&3||o)&&(c??=Zm(i,e,s),PA(i,n,e,o,t,r,s,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let d=0;d<u.length;d+=2){let f=u[d],h=u[d+1];c??=Zm(i,e,s),Wx(i,e,f,h,r,c)}if(l&&l.length)for(let d of l)c??=Zm(i,e,s),Wx(i,e,d,r,r,c)}}function vd(n=1){return gx(n)}function yd(n,e,t){return iI(n,e,t),yd}function Kg(n){let e=yt(),t=Tn(),i=Fm();ku(i+1);let r=$g(t,i);if(n.dirty&&q0(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=oI(e,i);n.reset(s,qD),n.notifyOnChanges()}return!0}return!1}function Jg(){return tI(yt(),Fm())}function zu(n,e){return n<<17|e<<2}function Ps(n){return n>>17&32767}function wI(n){return(n&2)==2}function CI(n,e){return n&131071|e<<17}function bg(n){return n|2}function Lo(n){return(n&131068)>>2}function Qm(n,e){return n&-131069|e<<2}function DI(n){return(n&1)===1}function Eg(n){return n|1}function TI(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=Ps(o),c=Lo(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||Eo(d,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let f=Ps(n[a+1]);n[i+1]=zu(f,a),f!==0&&(n[f+1]=Qm(n[f+1],i)),n[a+1]=CI(n[a+1],i)}else n[i+1]=zu(a,0),a!==0&&(n[a+1]=Qm(n[a+1],i)),a=i;else n[i+1]=zu(c,0),a===0?a=i:n[c+1]=Qm(n[c+1],i),c=i;l&&(n[i+1]=bg(n[i+1])),Zx(n,u,i,!0),Zx(n,u,i,!1),AI(e,u,n,i,s),o=zu(a,c),s?e.classBindings=o:e.styleBindings=o}function AI(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Eo(s,e)>=0&&(t[i+1]=Eg(t[i+1]))}function Zx(n,e,t,i){let r=n[t+1],s=e===null,o=i?Ps(r):Lo(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];II(c,e)&&(a=!0,n[o+1]=i?Eg(l):bg(l)),o=i?Ps(l):Lo(l)}a&&(n[t+1]=i?bg(r):Eg(r))}function II(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Eo(n,e)>=0:!1}function _d(n,e){return RI(n,e,null,!0),_d}function RI(n,e,t,i){let r=yt(),s=Tn(),o=cx(2);if(s.firstUpdatePass&&PI(s,n,o,i),e!==dr&&dd(r,o,e)){let a=s.data[Ts()];UI(s,a,r,r[Lt],n,r[o+1]=BI(e,t),i,o)}}function NI(n,e){return e>=n.expandoStartIndex}function PI(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Ts()],o=NI(n,t);VI(s,i)&&e===null&&!o&&(e=!1),e=OI(r,s,e,i),TI(r,s,e,t,o,i)}}function OI(n,e,t,i){let r=fx(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=eg(null,n,e,t,i),t=vc(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=eg(r,n,e,t,i),s===null){let c=LI(n,e,i);c!==void 0&&Array.isArray(c)&&(c=eg(null,n,e,c[1],i),c=vc(c,e.attrs,i),FI(n,e,i,c))}else s=kI(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function LI(n,e,t){let i=t?e.classBindings:e.styleBindings;if(Lo(i)!==0)return n[Ps(i)]}function FI(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[Ps(r)]=i}function kI(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=vc(i,o,t)}return vc(i,e.attrs,t)}function eg(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=vc(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function vc(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),k0(n,o,t?!0:e[++s]))}return n===void 0?null:n}function UI(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=DI(l)?Kx(c,e,t,r,Lo(l),o):void 0;if(!id(u)){id(s)||wI(l)&&(s=Kx(c,null,t,r,a,o));let d=wm(Ts(),t);UT(i,o,d,r,s)}}function Kx(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,d=u===null,f=t[r+1];f===dr&&(f=d?Pn:void 0);let h=d?Au(f,i):u===i?f:void 0;if(l&&!id(h)&&(h=Au(c,i)),id(h)&&(a=h,o))return a;let g=n[r+1];r=o?Ps(g):Lo(g)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=Au(c,i))}return a}function id(n){return n!==void 0}function BI(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=ir(CM(n)))),n}function VI(n,e){return(n.flags&(e?8:16))!==0}function Jn(n,e=""){let t=yt(),i=Tn(),r=n+Ln,s=i.firstCreatePass?jg(i,r,1,e,null):i.data[r],o=HI(i,t,s,e);t[r]=o,Vu()&&kg(i,t,o,s),To(s,!1)}var HI=(n,e,t,i)=>(Hu(!0),iT(e[Lt],i));function zI(n,e,t,i=""){return dd(n,Lu(),t)?e+I0(t)+i:dr}function xd(n){return Cc("",n),xd}function Cc(n,e,t){let i=yt(),r=zI(i,n,e,t);return r!==dr&&GI(i,Ts(),r),Cc}function GI(n,e,t){let i=wm(e,n);rT(n[Lt],i,t)}function Fs(n,e,t){Xg(e)&&(e=e());let i=yt(),r=Lu();if(dd(i,r,e)){let s=Tn(),o=Bm();GM(o,i,n,e,i[Lt],t)}return Fs}function Bo(n,e){let t=Xg(n);return t&&n.set(e),t}function ks(n,e){let t=yt(),i=Tn(),r=Fn();return Eb(i,t,t[Lt],r,n,e),ks}function Jx(n,e,t){let i=Tn();i.firstCreatePass&&Sb(e,i.data,i.blueprint,Di(n),t)}function Sb(n,e,t,i,r){if(n=on(n),Array.isArray(n))for(let s=0;s<n.length;s++)Sb(n[s],e,t,i,r);else{let s=Tn(),o=yt(),a=Fn(),c=xs(n)?n:on(n.provide),l=Mm(n),u=a.providerIndexes&1048575,d=a.directiveStart,f=a.providerIndexes>>20;if(xs(n)||!n.multi){let h=new Is(l,r,Tt,null),g=ng(c,e,r?u:u+f,d);g===-1?(rg(Zu(a,o),s,c),tg(s,n,e.length),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(h),o.push(h)):(t[g]=h,o[g]=h)}else{let h=ng(c,e,u+f,d),g=ng(c,e,u,u+f),_=h>=0&&t[h],m=g>=0&&t[g];if(r&&!m||!r&&!_){rg(Zu(a,o),s,c);let p=$I(r?WI:jI,t.length,r,i,l,n);!r&&m&&(t[g].providerFactory=p),tg(s,n,e.length,0),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=wb(t[r?g:h],l,!r&&i);tg(s,n,h>-1?h:g,p)}!r&&i&&m&&t[g].componentProviders++}}}function tg(n,e,t,i){let r=xs(e),s=z0(e);if(r||s){let c=(s?on(e.useClass):e).prototype.ngOnDestroy;if(c){let l=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=l.indexOf(t);u===-1?l.push(t,[i,c]):l[u+1].push(i,c)}else l.push(t,c)}}}function wb(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function ng(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function jI(n,e,t,i,r){return Sg(this.multi,[])}function WI(n,e,t,i,r){let s=this.multi,o;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=pc(i,i[Fe],this.providerFactory.index,r);o=c.slice(0,a),Sg(s,o);for(let l=a;l<c.length;l++)o.push(c[l])}else o=[],Sg(s,o);return o}function Sg(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function $I(n,e,t,i,r,s){let o=new Is(n,t,Tt,null);return o.multi=[],o.index=e,o.componentProviders=0,wb(o,r,i&&!t),o}function Vo(n,e){return t=>{t.providersResolver=(i,r)=>Jx(i,r?r(n):n,!1),e&&(t.viewProvidersResolver=(i,r)=>Jx(i,r?r(e):e,!0))}}function Qg(n,e){let t=ox()+n,i=yt();return i[t]===dr?RA(i,t,e()):NA(i,t)}var rd=class{ngModuleFactory;componentFactories;constructor(e,t){this.ngModuleFactory=e,this.componentFactories=t}},ev=(()=>{class n{compileModuleSync(t){return new nd(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=fm(t),s=IM(r.declarations).reduce((o,a)=>{let c=Ir(a);return c&&o.push(new Po(c)),o},[]);return new rd(i,s)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Cb=(()=>{class n{applicationErrorHandler=ie(Ri);appRef=ie(Ls);taskService=ie(Ur);ngZone=ie(Mn);zonelessEnabled=ie(uc);tracing=ie(xc,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new nn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ya):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(ie(Wm,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Mx:Hm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ya+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Db(){return[{provide:bo,useExisting:Cb},{provide:Mn,useClass:Za},{provide:uc,useValue:!0}]}function qI(){return typeof $localize<"u"&&$localize.locale||wc}var tv=new Te("",{factory:()=>ie(tv,{optional:!0,skipSelf:!0})||qI()});function Ho(n,e){return Xl(n,e?.equal)}var Rb=Symbol("InputSignalNode#UNSET"),cR=et(ae({},Yl),{transformFn:void 0,applyValueToInputSignal(n,e){Va(n,e)}});function Nb(n,e){let t=Object.create(cR);t.value=n,t.transformFn=e?.transform;function i(){if(ka(t),t.value===Rb){let r=null;throw new Ce(-950,r)}return t.value}return i[_n]=t,i}function Tb(n,e){return Nb(n,e)}function lR(n){return Nb(Rb,n)}var Pb=(Tb.required=lR,Tb);var nv=new Te(""),uR=new Te("");function Dc(n){return!n.moduleRef}function dR(n){let e=Dc(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Mn);return t.run(()=>{Dc(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(Ri),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),Dc(n)){let s=()=>e.destroy(),o=n.platformInjector.get(nv);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(nv);o.add(s),n.moduleRef.onDestroy(()=>{fc(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return hR(i,t,()=>{let s=e.get(Ur),o=s.add(),a=e.get(Zg);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(tv,wc);if(bb(c||wc),!e.get(uR,!0))return Dc(n)?e.get(Ls):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Dc(n)){let u=e.get(Ls);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return fR?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var fR;function hR(n,e,t){try{let i=t();return Os(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var Md=null;function pR(n=[],e){return ai.create({name:e,providers:[{provide:ec,useValue:"platform"},{provide:nv,useValue:new Set([()=>Md=null])},...n]})}function mR(n=[]){if(Md)return Md;let e=pR(n);return Md=e,xb(),gR(e),e}function gR(n){let e=n.get(od,null);dn(n,()=>{e?.forEach(t=>t())})}var vR=1e4;var j6=vR-1e3;var Us=(()=>{class n{static __NG_ELEMENT_ID__=yR}return n})();function yR(n){return _R(Fn(),yt(),(n&16)===16)}function _R(n,e,t){if(Fr(n)&&!t){let i=Kn(n.index,e);return new Br(i,i)}else if(n.type&175){let i=e[Yn];return new Br(i,e)}return null}var iv=class{supports(e){return Wg(e)}create(e){return new rv(e)}},xR=(n,e)=>e,rv=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(e){this._trackByFn=e||xR}forEachItem(e){let t;for(t=this._itHead;t!==null;t=t._next)e(t)}forEachOperation(e){let t=this._itHead,i=this._removalsHead,r=0,s=null;for(;t||i;){let o=!i||t&&t.currentIndex<Ab(i,r,s)?t:i,a=Ab(o,r,s),c=o.currentIndex;if(o===i)r--,i=i._nextRemoved;else if(t=t._next,o.previousIndex==null)r++;else{s||(s=[]);let l=a-r,u=c-r;if(l!=u){for(let f=0;f<l;f++){let h=f<s.length?s[f]:s[f]=0,g=h+f;u<=g&&g<l&&(s[f]=h+1)}let d=o.previousIndex;s[d]=u-l}}a!==c&&e(o,a,c)}}forEachPreviousItem(e){let t;for(t=this._previousItHead;t!==null;t=t._nextPrevious)e(t)}forEachAddedItem(e){let t;for(t=this._additionsHead;t!==null;t=t._nextAdded)e(t)}forEachMovedItem(e){let t;for(t=this._movesHead;t!==null;t=t._nextMoved)e(t)}forEachRemovedItem(e){let t;for(t=this._removalsHead;t!==null;t=t._nextRemoved)e(t)}forEachIdentityChange(e){let t;for(t=this._identityChangesHead;t!==null;t=t._nextIdentityChange)e(t)}diff(e){if(e==null&&(e=[]),!Wg(e))throw new Ce(900,!1);return this.check(e)?this:null}onDestroy(){}check(e){this._reset();let t=this._itHead,i=!1,r,s,o;if(Array.isArray(e)){this.length=e.length;for(let a=0;a<this.length;a++)s=e[a],o=this._trackByFn(a,s),t===null||!Object.is(t.trackById,o)?(t=this._mismatch(t,s,o,a),i=!0):(i&&(t=this._verifyReinsertion(t,s,o,a)),Object.is(t.item,s)||this._addIdentityChange(t,s)),t=t._next}else r=0,cb(e,a=>{o=this._trackByFn(r,a),t===null||!Object.is(t.trackById,o)?(t=this._mismatch(t,a,o,r),i=!0):(i&&(t=this._verifyReinsertion(t,a,o,r)),Object.is(t.item,a)||this._addIdentityChange(t,a)),t=t._next,r++}),this.length=r;return this._truncate(t),this.collection=e,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let e;for(e=this._previousItHead=this._itHead;e!==null;e=e._next)e._nextPrevious=e._next;for(e=this._additionsHead;e!==null;e=e._nextAdded)e.previousIndex=e.currentIndex;for(this._additionsHead=this._additionsTail=null,e=this._movesHead;e!==null;e=e._nextMoved)e.previousIndex=e.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(e,t,i,r){let s;return e===null?s=this._itTail:(s=e._prev,this._remove(e)),e=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),e!==null?(Object.is(e.item,t)||this._addIdentityChange(e,t),this._reinsertAfter(e,s,r)):(e=this._linkedRecords===null?null:this._linkedRecords.get(i,r),e!==null?(Object.is(e.item,t)||this._addIdentityChange(e,t),this._moveAfter(e,s,r)):e=this._addAfter(new sv(t,i),s,r)),e}_verifyReinsertion(e,t,i,r){let s=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return s!==null?e=this._reinsertAfter(s,e._prev,r):e.currentIndex!=r&&(e.currentIndex=r,this._addToMoves(e,r)),e}_truncate(e){for(;e!==null;){let t=e._next;this._addToRemovals(this._unlink(e)),e=t}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(e,t,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(e);let r=e._prevRemoved,s=e._nextRemoved;return r===null?this._removalsHead=s:r._nextRemoved=s,s===null?this._removalsTail=r:s._prevRemoved=r,this._insertAfter(e,t,i),this._addToMoves(e,i),e}_moveAfter(e,t,i){return this._unlink(e),this._insertAfter(e,t,i),this._addToMoves(e,i),e}_addAfter(e,t,i){return this._insertAfter(e,t,i),this._additionsTail===null?this._additionsTail=this._additionsHead=e:this._additionsTail=this._additionsTail._nextAdded=e,e}_insertAfter(e,t,i){let r=t===null?this._itHead:t._next;return e._next=r,e._prev=t,r===null?this._itTail=e:r._prev=e,t===null?this._itHead=e:t._next=e,this._linkedRecords===null&&(this._linkedRecords=new bd),this._linkedRecords.put(e),e.currentIndex=i,e}_remove(e){return this._addToRemovals(this._unlink(e))}_unlink(e){this._linkedRecords!==null&&this._linkedRecords.remove(e);let t=e._prev,i=e._next;return t===null?this._itHead=i:t._next=i,i===null?this._itTail=t:i._prev=t,e}_addToMoves(e,t){return e.previousIndex===t||(this._movesTail===null?this._movesTail=this._movesHead=e:this._movesTail=this._movesTail._nextMoved=e),e}_addToRemovals(e){return this._unlinkedRecords===null&&(this._unlinkedRecords=new bd),this._unlinkedRecords.put(e),e.currentIndex=null,e._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=e,e._prevRemoved=null):(e._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=e),e}_addIdentityChange(e,t){return e.item=t,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=e:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=e,e}},sv=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(e,t){this.item=e,this.trackById=t}},ov=class{_head=null;_tail=null;add(e){this._head===null?(this._head=this._tail=e,e._nextDup=null,e._prevDup=null):(this._tail._nextDup=e,e._prevDup=this._tail,e._nextDup=null,this._tail=e)}get(e,t){let i;for(i=this._head;i!==null;i=i._nextDup)if((t===null||t<=i.currentIndex)&&Object.is(i.trackById,e))return i;return null}remove(e){let t=e._prevDup,i=e._nextDup;return t===null?this._head=i:t._nextDup=i,i===null?this._tail=t:i._prevDup=t,this._head===null}},bd=class{map=new Map;put(e){let t=e.trackById,i=this.map.get(t);i||(i=new ov,this.map.set(t,i)),i.add(e)}get(e,t){let i=e,r=this.map.get(i);return r?r.get(e,t):null}remove(e){let t=e.trackById;return this.map.get(t).remove(e)&&this.map.delete(t),e}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function Ab(n,e,t){let i=n.previousIndex;if(i===null)return i;let r=0;return t&&i<t.length&&(r=t[i]),i+e+r}function Ib(){return new cv([new iv])}var cv=(()=>{class n{factories;static \u0275prov=Oe({token:n,providedIn:"root",factory:Ib});constructor(t){this.factories=t}static create(t,i){if(i!=null){let r=i.factories.slice();t=t.concat(r)}return new n(t)}static extend(t){return{provide:n,useFactory:()=>{let i=ie(n,{optional:!0,skipSelf:!0});return n.create(t,i||Ib())}}}find(t){let i=this.factories.find(r=>r.supports(t));if(i!=null)return i;throw new Ce(901,!1)}}return n})();function Ob(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;_t(ot.BootstrapApplicationStart);try{let s=r?.injector??mR(i),o=[Db(),Ex,...t||[]],a=new gc({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return dR({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{_t(ot.BootstrapApplicationEnd)}}function lv(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}var Lb=null;function Qn(){return Lb}function uv(n){Lb??=n}var Tc=class{},Ed=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:()=>ie(Fb),providedIn:"platform"})}return n})();var Fb=(()=>{class n extends Ed{_location;_history;_doc=ie(Gt);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Qn().getBaseHref(this._doc)}onPopState(t){let i=Qn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=Qn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function Bb(n,e){return n?e?n.endsWith("/")?e.startsWith("/")?n+e.slice(1):n+e:e.startsWith("/")?n+e:`${n}/${e}`:n:e}function kb(n){let e=n.search(/#|\?|$/);return n[e-1]==="/"?n.slice(0,e-1)+n.slice(e):n}function Gr(n){return n&&n[0]!=="?"?`?${n}`:n}var Sd=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:()=>ie(bR),providedIn:"root"})}return n})(),MR=new Te(""),bR=(()=>{class n extends Sd{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??ie(Gt).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Bb(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+Gr(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,s){let o=this.prepareExternalUrl(r+Gr(s));this._platformLocation.pushState(t,i,o)}replaceState(t,i,r,s){let o=this.prepareExternalUrl(r+Gr(s));this._platformLocation.replaceState(t,i,o)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||n)(qe(Ed),qe(MR,8))};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),zo=(()=>{class n{_subject=new Vt;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=wR(kb(Ub(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+Gr(i))}normalize(t){return n.stripTrailingSlash(SR(this._basePath,Ub(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+Gr(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+Gr(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Gr;static joinWithSlash=Bb;static stripTrailingSlash=kb;static \u0275fac=function(i){return new(i||n)(qe(Sd))};static \u0275prov=Oe({token:n,factory:()=>ER(),providedIn:"root"})}return n})();function ER(){return new zo(qe(Sd))}function SR(n,e){if(!n||!e.startsWith(n))return e;let t=e.substring(n.length);return t===""||["/",";","?","#"].includes(t[0])?t:e}function Ub(n){return n.replace(/\/index.html$/,"")}function wR(n){if(new RegExp("^(https?:)?//").test(n)){let[,t]=n.split(/\/\/[^\/]+/);return t}return n}var wd=class{$implicit;ngForOf;index;count;constructor(e,t,i,r){this.$implicit=e,this.ngForOf=t,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},Cd=(()=>{class n{_viewContainer;_template;_differs;set ngForOf(t){this._ngForOf=t,this._ngForOfDirty=!0}set ngForTrackBy(t){this._trackByFn=t}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(t,i,r){this._viewContainer=t,this._template=i,this._differs=r}set ngForTemplate(t){t&&(this._template=t)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let t=this._ngForOf;!this._differ&&t&&(this._differ=this._differs.find(t).create(this.ngForTrackBy))}if(this._differ){let t=this._differ.diff(this._ngForOf);t&&this._applyChanges(t)}}_applyChanges(t){let i=this._viewContainer;t.forEachOperation((r,s,o)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new wd(r.item,this._ngForOf,-1,-1),o===null?void 0:o);else if(o==null)i.remove(s===null?void 0:s);else if(s!==null){let a=i.get(s);i.move(a,o),Vb(a,r)}});for(let r=0,s=i.length;r<s;r++){let a=i.get(r).context;a.index=r,a.count=s,a.ngForOf=this._ngForOf}t.forEachIdentityChange(r=>{let s=i.get(r.currentIndex);Vb(s,r)})}static ngTemplateContextGuard(t,i){return!0}static \u0275fac=function(i){return new(i||n)(Tt(Hr),Tt(Rs),Tt(cv))};static \u0275dir=kn({type:n,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return n})();function Vb(n,e){n.context.$implicit=e.item}var Dd=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=zr({type:n});static \u0275inj=or({})}return n})();function dv(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var Ac=class{};var Hb="browser";var Ic=class{_doc;constructor(e){this._doc=e}manager},Td=(()=>{class n extends Ic{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(qe(Gt))};static \u0275prov=Oe({token:n,factory:n.\u0275fac})}return n})(),Rd=new Te(""),mv=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof Td));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof Td);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Ce(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(qe(Rd),qe(Mn))};static \u0275prov=Oe({token:n,factory:n.\u0275fac})}return n})(),fv="ng-app-id";function zb(n){for(let e of n)e.remove()}function Gb(n,e){let t=e.createElement("style");return t.textContent=n,t}function DR(n,e,t,i){let r=n.head?.querySelectorAll(`style[${fv}="${e}"],link[${fv}="${e}"]`);if(r)for(let s of r)s.removeAttribute(fv),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function pv(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var gv=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,DR(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,Gb);i?.forEach(r=>this.addUsage(r,this.external,pv))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(zb(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])zb(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,Gb(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,pv(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(qe(Gt),qe(sd),qe(ad,8),qe(_c))};static \u0275prov=Oe({token:n,factory:n.\u0275fac})}return n})(),hv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},vv=/%COMP%/g;var Wb="%COMP%",TR=`_nghost-${Wb}`,AR=`_ngcontent-${Wb}`,IR=!0,RR=new Te("",{factory:()=>IR});function NR(n){return AR.replace(vv,n)}function PR(n){return TR.replace(vv,n)}function $b(n,e){return e.map(t=>t.replace(vv,n))}var yv=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Rc(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Id?r.applyToHost(t):r instanceof Nc&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(i.encapsulation){case di.Emulated:s=new Id(c,l,i,this.appId,u,o,a,d);break;case di.ShadowDom:return new Ad(c,t,i,o,a,this.nonce,d,l);case di.ExperimentalIsolatedShadowDom:return new Ad(c,t,i,o,a,this.nonce,d);default:s=new Nc(c,l,i,u,o,a,d);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(qe(mv),qe(gv),qe(sd),qe(RR),qe(Gt),qe(Mn),qe(ad),qe(xc,8))};static \u0275prov=Oe({token:n,factory:n.\u0275fac})}return n})(),Rc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(hv[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(jb(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(jb(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Ce(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=hv[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=hv[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(lr.DashCase|lr.Important)?e.style.setProperty(t,i,r&lr.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&lr.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=Qn().getGlobalEventTarget(this.doc,e),!e))throw new Ce(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function jb(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Ad=class extends Rc{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=$b(i.id,l);for(let d of l){let f=document.createElement("style");o&&f.setAttribute("nonce",o),f.textContent=d,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let d of u){let f=pv(d,r);o&&f.setAttribute("nonce",o),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Nc=class extends Rc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?$b(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&No.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Id=class extends Nc{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=NR(l),this.hostAttr=PR(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var Nd=class n extends Tc{supportsDOMEvents=!0;static makeCurrent(){uv(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=OR();return t==null?null:LR(t)}resetBaseElement(){Pc=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return dv(document.cookie,e)}},Pc=null;function OR(){return Pc=Pc||document.head.querySelector("base"),Pc?Pc.getAttribute("href"):null}function LR(n){return new URL(n,document.baseURI).pathname}var FR=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac})}return n})(),qb=["alt","control","meta","shift"],kR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},UR={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},Xb=(()=>{class n extends Ic{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Qn().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),qb.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=kR[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),qb.forEach(o=>{if(o!==r){let a=UR[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(qe(Gt))};static \u0275prov=Oe({token:n,factory:n.\u0275fac})}return n})();async function _v(n,e,t){let i=ae({rootComponent:n},BR(e,t));return Ob(i)}function BR(n,e){return{platformRef:e?.platformRef,appProviders:[...jR,...n?.providers??[]],platformProviders:GR}}function VR(){Nd.makeCurrent()}function HR(){return new rr}function zR(){return Dg(document),document}var GR=[{provide:_c,useValue:Hb},{provide:od,useValue:VR,multi:!0},{provide:Gt,useFactory:zR}];var jR=[{provide:ec,useValue:"root"},{provide:rr,useFactory:HR},{provide:Rd,useClass:Td,multi:!0},{provide:Rd,useClass:Xb,multi:!0},yv,gv,mv,{provide:Ns,useExisting:yv},{provide:Ac,useClass:FR},[]];var Yb=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(qe(Gt))};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Ve="primary",jc=Symbol("RouteTitle"),Sv=class{params;constructor(e){this.params=e||{}}has(e){return Object.prototype.hasOwnProperty.call(this.params,e)}get(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t[0]:t}return null}getAll(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function qo(n){return new Sv(n)}function xv(n,e,t){for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(r[0]===":")t[r.substring(1)]=s;else if(r!==s.path)return!1}return!0}function $R(n,e,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>n.length||t.pathMatch==="full"&&(e.hasChildren()||i.length<n.length))return null;let c={},l=n.slice(0,i.length);return xv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let s=i.slice(0,r),o=i.slice(r+1);if(s.length+o.length>n.length||t.pathMatch==="full"&&e.hasChildren()&&t.path!=="**")return null;let a={};return!xv(s,n.slice(0,s.length),a)||!xv(o,n.slice(n.length-o.length),a)?null:{consumed:n,posParams:a}}function Ud(n){return new Promise((e,t)=>{n.pipe(er()).subscribe({next:i=>e(i),error:i=>t(i)})})}function qR(n,e){if(n.length!==e.length)return!1;for(let t=0;t<n.length;++t)if(!Li(n[t],e[t]))return!1;return!0}function Li(n,e){let t=n?wv(n):void 0,i=e?wv(e):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let s=0;s<t.length;s++)if(r=t[s],!sE(n[r],e[r]))return!1;return!0}function wv(n){return[...Object.keys(n),...Object.getOwnPropertySymbols(n)]}function sE(n,e){if(Array.isArray(n)&&Array.isArray(e)){if(n.length!==e.length)return!1;let t=[...n].sort(),i=[...e].sort();return t.every((r,s)=>i[s]===r)}else return n===e}function XR(n){return n.length>0?n[n.length-1]:null}function Gs(n){return pu(n)?n:Os(n)?Ut(Promise.resolve(n)):tt(n)}function oE(n){return pu(n)?Ud(n):Promise.resolve(n)}var YR={exact:cE,subset:lE},aE={exact:ZR,subset:KR,ignored:()=>!0};function Zb(n,e,t){return YR[t.paths](n.root,e.root,t.matrixParams)&&aE[t.queryParams](n.queryParams,e.queryParams)&&!(t.fragment==="exact"&&n.fragment!==e.fragment)}function ZR(n,e){return Li(n,e)}function cE(n,e,t){if(!Vs(n.segments,e.segments)||!Ld(n.segments,e.segments,t)||n.numberOfChildren!==e.numberOfChildren)return!1;for(let i in e.children)if(!n.children[i]||!cE(n.children[i],e.children[i],t))return!1;return!0}function KR(n,e){return Object.keys(e).length<=Object.keys(n).length&&Object.keys(e).every(t=>sE(n[t],e[t]))}function lE(n,e,t){return uE(n,e,e.segments,t)}function uE(n,e,t,i){if(n.segments.length>t.length){let r=n.segments.slice(0,t.length);return!(!Vs(r,t)||e.hasChildren()||!Ld(r,t,i))}else if(n.segments.length===t.length){if(!Vs(n.segments,t)||!Ld(n.segments,t,i))return!1;for(let r in e.children)if(!n.children[r]||!lE(n.children[r],e.children[r],i))return!1;return!0}else{let r=t.slice(0,n.segments.length),s=t.slice(n.segments.length);return!Vs(n.segments,r)||!Ld(n.segments,r,i)||!n.children[Ve]?!1:uE(n.children[Ve],e,s,i)}}function Ld(n,e,t){return e.every((i,r)=>aE[t](n[r].parameters,i.parameters))}var Fi=class{root;queryParams;fragment;_queryParamMap;constructor(e=new dt([],{}),t={},i=null){this.root=e,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=qo(this.queryParams),this._queryParamMap}toString(){return e1.serialize(this)}},dt=class{segments;children;parent=null;constructor(e,t){this.segments=e,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Fd(this)}},Bs=class{path;parameters;_parameterMap;constructor(e,t){this.path=e,this.parameters=t}get parameterMap(){return this._parameterMap??=qo(this.parameters),this._parameterMap}toString(){return fE(this)}};function JR(n,e){return Vs(n,e)&&n.every((t,i)=>Li(t.parameters,e[i].parameters))}function Vs(n,e){return n.length!==e.length?!1:n.every((t,i)=>t.path===e[i].path)}function QR(n,e){let t=[];return Object.entries(n.children).forEach(([i,r])=>{i===Ve&&(t=t.concat(e(r,i)))}),Object.entries(n.children).forEach(([i,r])=>{i!==Ve&&(t=t.concat(e(r,i)))}),t}var Xd=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:()=>new Hs,providedIn:"root"})}return n})(),Hs=class{parse(e){let t=new Dv(e);return new Fi(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(e){let t=`/${Oc(e.root,!0)}`,i=i1(e.queryParams),r=typeof e.fragment=="string"?`#${t1(e.fragment)}`:"";return`${t}${i}${r}`}},e1=new Hs;function Fd(n){return n.segments.map(e=>fE(e)).join("/")}function Oc(n,e){if(!n.hasChildren())return Fd(n);if(e){let t=n.children[Ve]?Oc(n.children[Ve],!1):"",i=[];return Object.entries(n.children).forEach(([r,s])=>{r!==Ve&&i.push(`${r}:${Oc(s,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=QR(n,(i,r)=>r===Ve?[Oc(n.children[Ve],!1)]:[`${r}:${Oc(i,!1)}`]);return Object.keys(n.children).length===1&&n.children[Ve]!=null?`${Fd(n)}/${t[0]}`:`${Fd(n)}/(${t.join("//")})`}}function dE(n){return encodeURIComponent(n).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Pd(n){return dE(n).replace(/%3B/gi,";")}function t1(n){return encodeURI(n)}function Cv(n){return dE(n).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function kd(n){return decodeURIComponent(n)}function Kb(n){return kd(n.replace(/\+/g,"%20"))}function fE(n){return`${Cv(n.path)}${n1(n.parameters)}`}function n1(n){return Object.entries(n).map(([e,t])=>`;${Cv(e)}=${Cv(t)}`).join("")}function i1(n){let e=Object.entries(n).map(([t,i])=>Array.isArray(i)?i.map(r=>`${Pd(t)}=${Pd(r)}`).join("&"):`${Pd(t)}=${Pd(i)}`).filter(t=>t);return e.length?`?${e.join("&")}`:""}var r1=/^[^\/()?;#]+/;function Mv(n){let e=n.match(r1);return e?e[0]:""}var s1=/^[^\/()?;=#]+/;function o1(n){let e=n.match(s1);return e?e[0]:""}var a1=/^[^=?&#]+/;function c1(n){let e=n.match(a1);return e?e[0]:""}var l1=/^[^&#]+/;function u1(n){let e=n.match(l1);return e?e[0]:""}var Dv=class{url;remaining;constructor(e){this.url=e,this.remaining=e}parseRootSegment(){return this.consumeOptional("/"),this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new dt([],{}):new dt([],this.parseChildren())}parseQueryParams(){let e={};if(this.consumeOptional("?"))do this.parseQueryParam(e);while(this.consumeOptional("&"));return e}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(e=0){if(e>50)throw new Ce(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,e));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,e)),(t.length>0||Object.keys(i).length>0)&&(r[Ve]=new dt(t,i)),r}parseSegment(){let e=Mv(this.remaining);if(e===""&&this.peekStartsWith(";"))throw new Ce(4009,!1);return this.capture(e),new Bs(kd(e),this.parseMatrixParams())}parseMatrixParams(){let e={};for(;this.consumeOptional(";");)this.parseParam(e);return e}parseParam(e){let t=o1(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=Mv(this.remaining);r&&(i=r,this.capture(i))}e[kd(t)]=kd(i)}parseQueryParam(e){let t=c1(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let o=u1(this.remaining);o&&(i=o,this.capture(i))}let r=Kb(t),s=Kb(i);if(e.hasOwnProperty(r)){let o=e[r];Array.isArray(o)||(o=[o],e[r]=o),o.push(s)}else e[r]=s}parseParens(e,t){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Mv(this.remaining),s=this.remaining[r.length];if(s!=="/"&&s!==")"&&s!==";")throw new Ce(4010,!1);let o;r.indexOf(":")>-1?(o=r.slice(0,r.indexOf(":")),this.capture(o),this.capture(":")):e&&(o=Ve);let a=this.parseChildren(t+1);i[o??Ve]=Object.keys(a).length===1&&a[Ve]?a[Ve]:new dt([],a),this.consumeOptional("//")}return i}peekStartsWith(e){return this.remaining.startsWith(e)}consumeOptional(e){return this.peekStartsWith(e)?(this.remaining=this.remaining.substring(e.length),!0):!1}capture(e){if(!this.consumeOptional(e))throw new Ce(4011,!1)}};function hE(n){return n.segments.length>0?new dt([],{[Ve]:n}):n}function pE(n){let e={};for(let[i,r]of Object.entries(n.children)){let s=pE(r);if(i===Ve&&s.segments.length===0&&s.hasChildren())for(let[o,a]of Object.entries(s.children))e[o]=a;else(s.segments.length>0||s.hasChildren())&&(e[i]=s)}let t=new dt(n.segments,e);return d1(t)}function d1(n){if(n.numberOfChildren===1&&n.children[Ve]){let e=n.children[Ve];return new dt(n.segments.concat(e.segments),e.children)}return n}function Xo(n){return n instanceof Fi}function f1(n,e,t=null,i=null,r=new Hs){let s=mE(n);return gE(s,e,t,i,r)}function mE(n){let e;function t(s){let o={};for(let c of s.children){let l=t(c);o[c.outlet]=l}let a=new dt(s.url,o);return s===n&&(e=a),a}let i=t(n.root),r=hE(i);return e??r}function gE(n,e,t,i,r){let s=n;for(;s.parent;)s=s.parent;if(e.length===0)return bv(s,s,s,t,i,r);let o=h1(e);if(o.toRoot())return bv(s,s,new dt([],{}),t,i,r);let a=p1(o,s,n),c=a.processChildren?Fc(a.segmentGroup,a.index,o.commands):yE(a.segmentGroup,a.index,o.commands);return bv(s,a.segmentGroup,c,t,i,r)}function Bd(n){return typeof n=="object"&&n!=null&&!n.outlets&&!n.segmentPath}function Bc(n){return typeof n=="object"&&n!=null&&n.outlets}function Jb(n,e,t){n||="\u0275";let i=new Fi;return i.queryParams={[n]:e},t.parse(t.serialize(i)).queryParams[n]}function bv(n,e,t,i,r,s){let o={};for(let[l,u]of Object.entries(i??{}))o[l]=Array.isArray(u)?u.map(d=>Jb(l,d,s)):Jb(l,u,s);let a;n===e?a=t:a=vE(n,e,t);let c=hE(pE(a));return new Fi(c,o,r)}function vE(n,e,t){let i={};return Object.entries(n.children).forEach(([r,s])=>{s===e?i[r]=t:i[r]=vE(s,e,t)}),new dt(n.segments,i)}var Vd=class{isAbsolute;numberOfDoubleDots;commands;constructor(e,t,i){if(this.isAbsolute=e,this.numberOfDoubleDots=t,this.commands=i,e&&i.length>0&&Bd(i[0]))throw new Ce(4003,!1);let r=i.find(Bc);if(r&&r!==XR(i))throw new Ce(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function h1(n){if(typeof n[0]=="string"&&n.length===1&&n[0]==="/")return new Vd(!0,0,n);let e=0,t=!1,i=n.reduce((r,s,o)=>{if(typeof s=="object"&&s!=null){if(s.outlets){let a={};return Object.entries(s.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(s.segmentPath)return[...r,s.segmentPath]}return typeof s!="string"?[...r,s]:o===0?(s.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?e++:a!=""&&r.push(a))}),r):[...r,s]},[]);return new Vd(t,e,i)}var jo=class{segmentGroup;processChildren;index;constructor(e,t,i){this.segmentGroup=e,this.processChildren=t,this.index=i}};function p1(n,e,t){if(n.isAbsolute)return new jo(e,!0,0);if(!t)return new jo(e,!1,NaN);if(t.parent===null)return new jo(t,!0,0);let i=Bd(n.commands[0])?0:1,r=t.segments.length-1+i;return m1(t,r,n.numberOfDoubleDots)}function m1(n,e,t){let i=n,r=e,s=t;for(;s>r;){if(s-=r,i=i.parent,!i)throw new Ce(4005,!1);r=i.segments.length}return new jo(i,!1,r-s)}function g1(n){return Bc(n[0])?n[0].outlets:{[Ve]:n}}function yE(n,e,t){if(n??=new dt([],{}),n.segments.length===0&&n.hasChildren())return Fc(n,e,t);let i=v1(n,e,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<n.segments.length){let s=new dt(n.segments.slice(0,i.pathIndex),{});return s.children[Ve]=new dt(n.segments.slice(i.pathIndex),n.children),Fc(s,0,r)}else return i.match&&r.length===0?new dt(n.segments,{}):i.match&&!n.hasChildren()?Tv(n,e,t):i.match?Fc(n,0,r):Tv(n,e,t)}function Fc(n,e,t){if(t.length===0)return new dt(n.segments,{});{let i=g1(t),r={};if(Object.keys(i).some(s=>s!==Ve)&&n.children[Ve]&&n.numberOfChildren===1&&n.children[Ve].segments.length===0){let s=Fc(n.children[Ve],e,t);return new dt(n.segments,s.children)}return Object.entries(i).forEach(([s,o])=>{typeof o=="string"&&(o=[o]),o!==null&&(r[s]=yE(n.children[s],e,o))}),Object.entries(n.children).forEach(([s,o])=>{i[s]===void 0&&(r[s]=o)}),new dt(n.segments,r)}}function v1(n,e,t){let i=0,r=e,s={match:!1,pathIndex:0,commandIndex:0};for(;r<n.segments.length;){if(i>=t.length)return s;let o=n.segments[r],a=t[i];if(Bc(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!eE(c,l,o))return s;i+=2}else{if(!eE(c,{},o))return s;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Tv(n,e,t){let i=n.segments.slice(0,e),r=0;for(;r<t.length;){let s=t[r];if(Bc(s)){let c=y1(s.outlets);return new dt(i,c)}if(r===0&&Bd(t[0])){let c=n.segments[e];i.push(new Bs(c.path,Qb(t[0]))),r++;continue}let o=Bc(s)?s.outlets[Ve]:`${s}`,a=r<t.length-1?t[r+1]:null;o&&a&&Bd(a)?(i.push(new Bs(o,Qb(a))),r+=2):(i.push(new Bs(o,{})),r++)}return new dt(i,{})}function y1(n){let e={};return Object.entries(n).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(e[t]=Tv(new dt([],{}),0,i))}),e}function Qb(n){let e={};return Object.entries(n).forEach(([t,i])=>e[t]=`${i}`),e}function eE(n,e,t){return n==t.path&&Li(e,t.parameters)}var kc="imperative",an=(function(n){return n[n.NavigationStart=0]="NavigationStart",n[n.NavigationEnd=1]="NavigationEnd",n[n.NavigationCancel=2]="NavigationCancel",n[n.NavigationError=3]="NavigationError",n[n.RoutesRecognized=4]="RoutesRecognized",n[n.ResolveStart=5]="ResolveStart",n[n.ResolveEnd=6]="ResolveEnd",n[n.GuardsCheckStart=7]="GuardsCheckStart",n[n.GuardsCheckEnd=8]="GuardsCheckEnd",n[n.RouteConfigLoadStart=9]="RouteConfigLoadStart",n[n.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",n[n.ChildActivationStart=11]="ChildActivationStart",n[n.ChildActivationEnd=12]="ChildActivationEnd",n[n.ActivationStart=13]="ActivationStart",n[n.ActivationEnd=14]="ActivationEnd",n[n.Scroll=15]="Scroll",n[n.NavigationSkipped=16]="NavigationSkipped",n})(an||{}),ei=class{id;url;constructor(e,t){this.id=e,this.url=t}},Yo=class extends ei{type=an.NavigationStart;navigationTrigger;restoredState;constructor(e,t,i="imperative",r=null){super(e,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},jr=class extends ei{urlAfterRedirects;type=an.NavigationEnd;constructor(e,t,i){super(e,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},En=(function(n){return n[n.Redirect=0]="Redirect",n[n.SupersededByNewNavigation=1]="SupersededByNewNavigation",n[n.NoDataFromResolver=2]="NoDataFromResolver",n[n.GuardRejected=3]="GuardRejected",n[n.Aborted=4]="Aborted",n})(En||{}),Hd=(function(n){return n[n.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",n[n.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",n})(Hd||{}),hi=class extends ei{reason;code;type=an.NavigationCancel;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function _E(n){return n instanceof hi&&(n.code===En.Redirect||n.code===En.SupersededByNewNavigation)}var Wr=class extends ei{reason;code;type=an.NavigationSkipped;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}},Zo=class extends ei{error;target;type=an.NavigationError;constructor(e,t,i,r){super(e,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Vc=class extends ei{urlAfterRedirects;state;type=an.RoutesRecognized;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Av=class extends ei{urlAfterRedirects;state;type=an.GuardsCheckStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Iv=class extends ei{urlAfterRedirects;state;shouldActivate;type=an.GuardsCheckEnd;constructor(e,t,i,r,s){super(e,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=s}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Rv=class extends ei{urlAfterRedirects;state;type=an.ResolveStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Nv=class extends ei{urlAfterRedirects;state;type=an.ResolveEnd;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Pv=class{route;type=an.RouteConfigLoadStart;constructor(e){this.route=e}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Ov=class{route;type=an.RouteConfigLoadEnd;constructor(e){this.route=e}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Lv=class{snapshot;type=an.ChildActivationStart;constructor(e){this.snapshot=e}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Fv=class{snapshot;type=an.ChildActivationEnd;constructor(e){this.snapshot=e}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},kv=class{snapshot;type=an.ActivationStart;constructor(e){this.snapshot=e}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Uv=class{snapshot;type=an.ActivationEnd;constructor(e){this.snapshot=e}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Ko=class{},Jo=class{url;navigationBehaviorOptions;constructor(e,t){this.url=e,this.navigationBehaviorOptions=t}};function _1(n){return!(n instanceof Ko)&&!(n instanceof Jo)}var Bv=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(e){this.rootInjector=e,this.children=new Wc(this.rootInjector)}},Wc=(()=>{class n{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new Bv(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||n)(qe(zt))};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),zd=class{_root;constructor(e){this._root=e}get root(){return this._root.value}parent(e){let t=this.pathFromRoot(e);return t.length>1?t[t.length-2]:null}children(e){let t=Vv(e,this._root);return t?t.children.map(i=>i.value):[]}firstChild(e){let t=Vv(e,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(e){let t=Hv(e,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==e)}pathFromRoot(e){return Hv(e,this._root).map(t=>t.value)}};function Vv(n,e){if(n===e.value)return e;for(let t of e.children){let i=Vv(n,t);if(i)return i}return null}function Hv(n,e){if(n===e.value)return[e];for(let t of e.children){let i=Hv(n,t);if(i.length)return i.unshift(e),i}return[]}var Un=class{value;children;constructor(e,t){this.value=e,this.children=t}toString(){return`TreeNode(${this.value})`}};function Go(n){let e={};return n&&n.children.forEach(t=>e[t.value.outlet]=t),e}var Gd=class extends zd{snapshot;constructor(e,t){super(e),this.snapshot=t,Zv(this,e)}toString(){return this.snapshot.toString()}};function xE(n,e){let t=x1(n,e),i=new rn([new Bs("",{})]),r=new rn({}),s=new rn({}),o=new rn({}),a=new rn(""),c=new zs(i,r,o,a,s,Ve,n,t.root);return c.snapshot=t.root,new Gd(new Un(c,[]),t)}function x1(n,e){let t={},i={},r={},o=new Wo([],t,r,"",i,Ve,n,null,{},e);return new Wd("",new Un(o,[]))}var zs=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(e,t,i,r,s,o,a,c){this.urlSubject=e,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=s,this.outlet=o,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(Dt(l=>l[jc]))??tt(void 0),this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(Dt(e=>qo(e))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(Dt(e=>qo(e))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function jd(n,e,t="emptyOnly"){let i,{routeConfig:r}=n;return e!==null&&(t==="always"||r?.path===""||!e.component&&!e.routeConfig?.loadComponent)?i={params:ae(ae({},e.params),n.params),data:ae(ae({},e.data),n.data),resolve:ae(ae(ae(ae({},n.data),e.data),r?.data),n._resolvedData)}:i={params:ae({},n.params),data:ae({},n.data),resolve:ae(ae({},n.data),n._resolvedData??{})},r&&bE(r)&&(i.resolve[jc]=r.title),i}var Wo=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[jc]}constructor(e,t,i,r,s,o,a,c,l,u){this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s,this.outlet=o,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=qo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=qo(this.queryParams),this._queryParamMap}toString(){let e=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${e}', path:'${t}')`}},Wd=class extends zd{url;constructor(e,t){super(t),this.url=e,Zv(this,t)}toString(){return ME(this._root)}};function Zv(n,e){e.value._routerState=n,e.children.forEach(t=>Zv(n,t))}function ME(n){let e=n.children.length>0?` { ${n.children.map(ME).join(", ")} } `:"";return`${n.value}${e}`}function Ev(n){if(n.snapshot){let e=n.snapshot,t=n._futureSnapshot;n.snapshot=t,Li(e.queryParams,t.queryParams)||n.queryParamsSubject.next(t.queryParams),e.fragment!==t.fragment&&n.fragmentSubject.next(t.fragment),Li(e.params,t.params)||n.paramsSubject.next(t.params),qR(e.url,t.url)||n.urlSubject.next(t.url),Li(e.data,t.data)||n.dataSubject.next(t.data)}else n.snapshot=n._futureSnapshot,n.dataSubject.next(n._futureSnapshot.data)}function zv(n,e){let t=Li(n.params,e.params)&&JR(n.url,e.url),i=!n.parent!=!e.parent;return t&&!i&&(!n.parent||zv(n.parent,e.parent))}function bE(n){return typeof n.title=="string"||n.title===null}var M1=new Te(""),EE=(()=>{class n{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=Ve;activateEvents=new Ht;deactivateEvents=new Ht;attachEvents=new Ht;detachEvents=new Ht;routerOutletData=Pb();parentContexts=ie(Wc);location=ie(Hr);changeDetector=ie(Us);inputBinder=ie(Yd,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new Ce(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new Ce(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new Ce(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new Ce(4013,!1);this._activatedRoute=t;let r=this.location,o=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Gv(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(o,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=kn({type:n,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Fo]})}return n})(),Gv=class{route;childContexts;parent;outletData;constructor(e,t,i,r){this.route=e,this.childContexts=t,this.parent=i,this.outletData=r}get(e,t){return e===zs?this.route:e===Wc?this.childContexts:e===M1?this.outletData:this.parent.get(e,t)}},Yd=new Te("");var SE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=Sc({type:n,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&Uo(0,"router-outlet")},dependencies:[EE],encapsulation:2})}return n})();function Kv(n){let e=n.children&&n.children.map(Kv),t=e?et(ae({},n),{children:e}):ae({},n);return!t.component&&!t.loadComponent&&(e||t.loadChildren)&&t.outlet&&t.outlet!==Ve&&(t.component=SE),t}function b1(n,e,t){let i=Hc(n,e._root,t?t._root:void 0);return new Gd(i,e)}function Hc(n,e,t){if(t&&n.shouldReuseRoute(e.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=e.value;let r=E1(n,e,t);return new Un(i,r)}else{if(n.shouldAttach(e.value)){let s=n.retrieve(e.value);if(s!==null){let o=s.route;return o.value._futureSnapshot=e.value,o.children=e.children.map(a=>Hc(n,a)),o}}let i=S1(e.value),r=e.children.map(s=>Hc(n,s));return new Un(i,r)}}function E1(n,e,t){return e.children.map(i=>{for(let r of t.children)if(n.shouldReuseRoute(i.value,r.value.snapshot))return Hc(n,i,r);return Hc(n,i)})}function S1(n){return new zs(new rn(n.url),new rn(n.params),new rn(n.queryParams),new rn(n.fragment),new rn(n.data),n.outlet,n.component,n)}var zc=class{redirectTo;navigationBehaviorOptions;constructor(e,t){this.redirectTo=e,this.navigationBehaviorOptions=t}},wE="ngNavigationCancelingError";function $d(n,e){let{redirectTo:t,navigationBehaviorOptions:i}=Xo(e)?{redirectTo:e,navigationBehaviorOptions:void 0}:e,r=CE(!1,En.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function CE(n,e){let t=new Error(`NavigationCancelingError: ${n||""}`);return t[wE]=!0,t.cancellationCode=e,t}function w1(n){return DE(n)&&Xo(n.url)}function DE(n){return!!n&&n[wE]}var jv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(e,t,i,r,s){this.routeReuseStrategy=e,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=s}activate(e){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,e),Ev(this.futureState.root),this.activateChildRoutes(t,i,e)}deactivateChildRoutes(e,t,i){let r=Go(t);e.children.forEach(s=>{let o=s.value.outlet;this.deactivateRoutes(s,r[o],i),delete r[o]}),Object.values(r).forEach(s=>{this.deactivateRouteAndItsChildren(s,i)})}deactivateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(r===s)if(r.component){let o=i.getContext(r.outlet);o&&this.deactivateChildRoutes(e,t,o.children)}else this.deactivateChildRoutes(e,t,i);else s&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(e,t){e.value.component&&this.routeReuseStrategy.shouldDetach(e.value.snapshot)?this.detachAndStoreRouteSubtree(e,t):this.deactivateRouteAndOutlet(e,t)}detachAndStoreRouteSubtree(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=Go(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);if(i&&i.outlet){let o=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(e.value.snapshot,{componentRef:o,route:e,contexts:a})}}deactivateRouteAndOutlet(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=Go(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(e,t,i){let r=Go(t);e.children.forEach(s=>{this.activateRoutes(s,r[s.value.outlet],i),this.forwardEvent(new Uv(s.value.snapshot))}),e.children.length&&this.forwardEvent(new Fv(e.value.snapshot))}activateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(Ev(r),r===s)if(r.component){let o=i.getOrCreateContext(r.outlet);this.activateChildRoutes(e,t,o.children)}else this.activateChildRoutes(e,t,i);else if(r.component){let o=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),o.children.onOutletReAttached(a.contexts),o.attachRef=a.componentRef,o.route=a.route.value,o.outlet&&o.outlet.attach(a.componentRef,a.route.value),Ev(a.route.value),this.activateChildRoutes(e,null,o.children)}else o.attachRef=null,o.route=r,o.outlet&&o.outlet.activateWith(r,o.injector),this.activateChildRoutes(e,null,o.children)}else this.activateChildRoutes(e,null,i)}},qd=class{path;route;constructor(e){this.path=e,this.route=this.path[this.path.length-1]}},$o=class{component;route;constructor(e,t){this.component=e,this.route=t}};function C1(n,e,t){let i=n._root,r=e?e._root:null;return Lc(i,r,t,[i.value])}function D1(n){let e=n.routeConfig?n.routeConfig.canActivateChild:null;return!e||e.length===0?null:{node:n,guards:e}}function ea(n,e){let t=Symbol(),i=e.get(n,t);return i===t?typeof n=="function"&&!om(n)?n:e.get(n):i}function Lc(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=Go(e);return n.children.forEach(o=>{T1(o,s[o.value.outlet],t,i.concat([o.value]),r),delete s[o.value.outlet]}),Object.entries(s).forEach(([o,a])=>Uc(a,t.getContext(o),r)),r}function T1(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=n.value,o=e?e.value:null,a=t?t.getContext(n.value.outlet):null;if(o&&s.routeConfig===o.routeConfig){let c=A1(o,s,s.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new qd(i)):(s.data=o.data,s._resolvedData=o._resolvedData),s.component?Lc(n,e,a?a.children:null,i,r):Lc(n,e,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new $o(a.outlet.component,o))}else o&&Uc(e,a,r),r.canActivateChecks.push(new qd(i)),s.component?Lc(n,null,a?a.children:null,i,r):Lc(n,null,t,i,r);return r}function A1(n,e,t){if(typeof t=="function")return dn(e._environmentInjector,()=>t(n,e));switch(t){case"pathParamsChange":return!Vs(n.url,e.url);case"pathParamsOrQueryParamsChange":return!Vs(n.url,e.url)||!Li(n.queryParams,e.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!zv(n,e)||!Li(n.queryParams,e.queryParams);default:return!zv(n,e)}}function Uc(n,e,t){let i=Go(n),r=n.value;Object.entries(i).forEach(([s,o])=>{r.component?e?Uc(o,e.children.getContext(s),t):Uc(o,null,t):Uc(o,e,t)}),r.component?e&&e.outlet&&e.outlet.isActivated?t.canDeactivateChecks.push(new $o(e.outlet.component,r)):t.canDeactivateChecks.push(new $o(null,r)):t.canDeactivateChecks.push(new $o(null,r))}function $c(n){return typeof n=="function"}function I1(n){return typeof n=="boolean"}function R1(n){return n&&$c(n.canLoad)}function N1(n){return n&&$c(n.canActivate)}function P1(n){return n&&$c(n.canActivateChild)}function O1(n){return n&&$c(n.canDeactivate)}function L1(n){return n&&$c(n.canMatch)}function TE(n){return n instanceof gs||n?.name==="EmptyError"}var Od=Symbol("INITIAL_VALUE");function Qo(){return tr(n=>Up(n.map(e=>e.pipe(Qi(1),Hp(Od)))).pipe(Dt(e=>{for(let t of e)if(t!==!0){if(t===Od)return Od;if(t===!1||F1(t))return t}return!0}),Ji(e=>e!==Od),Qi(1)))}function F1(n){return Xo(n)||n instanceof zc}function AE(n){return n.aborted?tt(void 0).pipe(Qi(1)):new rt(e=>{let t=()=>{e.next(),e.complete()};return n.addEventListener("abort",t),()=>n.removeEventListener("abort",t)})}function IE(n){return Wa(AE(n))}function k1(n){return xn(e=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:s}}=e;return s.length===0&&r.length===0?tt(et(ae({},e),{guardsResult:!0})):U1(s,t,i).pipe(xn(o=>o&&I1(o)?B1(t,r,n):tt(o)),Dt(o=>et(ae({},e),{guardsResult:o})))})}function U1(n,e,t){return Ut(n).pipe(xn(i=>j1(i.component,i.route,t,e)),er(i=>i!==!0,!0))}function B1(n,e,t){return Ut(e).pipe(yu(i=>_o(H1(i.route.parent,t),V1(i.route,t),G1(n,i.path),z1(n,i.route))),er(i=>i!==!0,!0))}function V1(n,e){return n!==null&&e&&e(new kv(n)),tt(!0)}function H1(n,e){return n!==null&&e&&e(new Lv(n)),tt(!0)}function z1(n,e){let t=e.routeConfig?e.routeConfig.canActivate:null;if(!t||t.length===0)return tt(!0);let i=t.map(r=>Ga(()=>{let s=e._environmentInjector,o=ea(r,s),a=N1(o)?o.canActivate(e,n):dn(s,()=>o(e,n));return Gs(a).pipe(er())}));return tt(i).pipe(Qo())}function G1(n,e){let t=e[e.length-1],r=e.slice(0,e.length-1).reverse().map(s=>D1(s)).filter(s=>s!==null).map(s=>Ga(()=>{let o=s.guards.map(a=>{let c=s.node._environmentInjector,l=ea(a,c),u=P1(l)?l.canActivateChild(t,n):dn(c,()=>l(t,n));return Gs(u).pipe(er())});return tt(o).pipe(Qo())}));return tt(r).pipe(Qo())}function j1(n,e,t,i){let r=e&&e.routeConfig?e.routeConfig.canDeactivate:null;if(!r||r.length===0)return tt(!0);let s=r.map(o=>{let a=e._environmentInjector,c=ea(o,a),l=O1(c)?c.canDeactivate(n,e,t,i):dn(a,()=>c(n,e,t,i));return Gs(l).pipe(er())});return tt(s).pipe(Qo())}function W1(n,e,t,i,r){let s=e.canLoad;if(s===void 0||s.length===0)return tt(!0);let o=s.map(a=>{let c=ea(a,n),l=R1(c)?c.canLoad(e,t):dn(n,()=>c(e,t)),u=Gs(l);return r?u.pipe(IE(r)):u});return tt(o).pipe(Qo(),RE(i))}function RE(n){return Op(Ei(e=>{if(typeof e!="boolean")throw $d(n,e)}),Dt(e=>e===!0))}function $1(n,e,t,i,r){let s=e.canMatch;if(!s||s.length===0)return tt(!0);let o=s.map(a=>{let c=ea(a,n),l=L1(c)?c.canMatch(e,t):dn(n,()=>c(e,t));return Gs(l).pipe(IE(r))});return tt(o).pipe(Qo(),RE(i))}var mr=class n extends Error{segmentGroup;constructor(e){super(),this.segmentGroup=e||null,Object.setPrototypeOf(this,n.prototype)}},Gc=class n extends Error{urlTree;constructor(e){super(),this.urlTree=e,Object.setPrototypeOf(this,n.prototype)}};function q1(n){throw new Ce(4e3,!1)}function X1(n){throw CE(!1,En.GuardRejected)}var Wv=class{urlSerializer;urlTree;constructor(e,t){this.urlSerializer=e,this.urlTree=t}async lineralizeSegments(e,t){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[Ve])throw q1(`${e.redirectTo}`);r=r.children[Ve]}}async applyRedirectCommands(e,t,i,r,s){let o=await Y1(t,r,s);if(o instanceof Fi)throw new Gc(o);let a=this.applyRedirectCreateUrlTree(o,this.urlSerializer.parse(o),e,i);if(o[0]==="/")throw new Gc(a);return a}applyRedirectCreateUrlTree(e,t,i,r){let s=this.createSegmentGroup(e,t.root,i,r);return new Fi(s,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(e,t){let i={};return Object.entries(e).forEach(([r,s])=>{if(typeof s=="string"&&s[0]===":"){let a=s.substring(1);i[r]=t[a]}else i[r]=s}),i}createSegmentGroup(e,t,i,r){let s=this.createSegments(e,t.segments,i,r),o={};return Object.entries(t.children).forEach(([a,c])=>{o[a]=this.createSegmentGroup(e,c,i,r)}),new dt(s,o)}createSegments(e,t,i,r){return t.map(s=>s.path[0]===":"?this.findPosParam(e,s,r):this.findOrReturn(s,i))}findPosParam(e,t,i){let r=i[t.path.substring(1)];if(!r)throw new Ce(4001,!1);return r}findOrReturn(e,t){let i=0;for(let r of t){if(r.path===e.path)return t.splice(i),r;i++}return e}};function Y1(n,e,t){if(typeof n=="string")return Promise.resolve(n);let i=n,{queryParams:r,fragment:s,routeConfig:o,url:a,outlet:c,params:l,data:u,title:d,paramMap:f,queryParamMap:h}=e;return Ud(Gs(dn(t,()=>i({params:l,data:u,queryParams:r,fragment:s,routeConfig:o,url:a,outlet:c,title:d,paramMap:f,queryParamMap:h}))))}function Z1(n,e){return n.providers&&!n._injector&&(n._injector=Ec(n.providers,e,`Route: ${n.path}`)),n._injector??e}function fi(n){return n.outlet||Ve}function K1(n,e){let t=n.filter(i=>fi(i)===e);return t.push(...n.filter(i=>fi(i)!==e)),t}var $v={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function J1(n,e,t,i,r,s){let o=NE(n,e,t);return o.matched?(i=Z1(e,i),$1(i,e,t,r,s).pipe(Dt(a=>a===!0?o:ae({},$v)))):tt(o)}function NE(n,e,t){if(e.path==="")return e.pathMatch==="full"&&(n.hasChildren()||t.length>0)?ae({},$v):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(e.matcher||$R)(t,n,e);if(!r)return ae({},$v);let s={};Object.entries(r.posParams??{}).forEach(([a,c])=>{s[a]=c.path});let o=r.consumed.length>0?ae(ae({},s),r.consumed[r.consumed.length-1].parameters):s;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:o,positionalParamSegments:r.posParams??{}}}function tE(n,e,t,i){return t.length>0&&tN(n,t,i)?{segmentGroup:new dt(e,eN(i,new dt(t,n.children))),slicedSegments:[]}:t.length===0&&nN(n,t,i)?{segmentGroup:new dt(n.segments,Q1(n,t,i,n.children)),slicedSegments:t}:{segmentGroup:new dt(n.segments,n.children),slicedSegments:t}}function Q1(n,e,t,i){let r={};for(let s of t)if(Zd(n,e,s)&&!i[fi(s)]){let o=new dt([],{});r[fi(s)]=o}return ae(ae({},i),r)}function eN(n,e){let t={};t[Ve]=e;for(let i of n)if(i.path===""&&fi(i)!==Ve){let r=new dt([],{});t[fi(i)]=r}return t}function tN(n,e,t){return t.some(i=>Zd(n,e,i)&&fi(i)!==Ve)}function nN(n,e,t){return t.some(i=>Zd(n,e,i))}function Zd(n,e,t){return(n.hasChildren()||e.length>0)&&t.pathMatch==="full"?!1:t.path===""}function iN(n,e,t){return e.length===0&&!n.children[t]}var qv=class{};async function rN(n,e,t,i,r,s,o="emptyOnly",a){return new Xv(n,e,t,i,r,o,s,a).recognize()}var sN=31,Xv=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(e,t,i,r,s,o,a,c){this.injector=e,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=s,this.paramsInheritanceStrategy=o,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Wv(this.urlSerializer,this.urlTree)}noMatchError(e){return new Ce(4002,`'${e.segmentGroup}'`)}async recognize(){let e=tE(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=await this.match(e),r=new Un(i,t),s=new Wd("",r),o=f1(i,[],this.urlTree.queryParams,this.urlTree.fragment);return o.queryParams=this.urlTree.queryParams,s.url=this.urlSerializer.serialize(o),{state:s,tree:o}}async match(e){let t=new Wo([],Object.freeze({}),Object.freeze(ae({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),Ve,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,e,Ve,t),rootSnapshot:t}}catch(i){if(i instanceof Gc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof mr?this.noMatchError(i):i}}async processSegmentGroup(e,t,i,r,s){if(i.segments.length===0&&i.hasChildren())return this.processChildren(e,t,i,s);let o=await this.processSegment(e,t,i,i.segments,r,!0,s);return o instanceof Un?[o]:[]}async processChildren(e,t,i,r){let s=[];for(let c of Object.keys(i.children))c==="primary"?s.unshift(c):s.push(c);let o=[];for(let c of s){let l=i.children[c],u=K1(t,c),d=await this.processSegmentGroup(e,u,l,c,r);o.push(...d)}let a=PE(o);return oN(a),a}async processSegment(e,t,i,r,s,o,a){for(let c of t)try{return await this.processSegmentAgainstRoute(c._injector??e,t,c,i,r,s,o,a)}catch(l){if(l instanceof mr||TE(l))continue;throw l}if(iN(i,r,s))return new qv;throw new mr(i)}async processSegmentAgainstRoute(e,t,i,r,s,o,a,c){if(fi(i)!==o&&(o===Ve||!Zd(r,s,i)))throw new mr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(e,r,i,s,o,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(e,r,t,i,s,o,c);throw new mr(r)}async expandSegmentAgainstRouteUsingRedirect(e,t,i,r,s,o,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:d,remainingSegments:f}=NE(t,r,s);if(!c)throw new mr(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>sN&&(this.allowRedirects=!1));let h=new Wo(s,l,Object.freeze(ae({},this.urlTree.queryParams)),this.urlTree.fragment,nE(r),fi(r),r.component??r._loadedComponent??null,r,iE(r),e),g=jd(h,a,this.paramsInheritanceStrategy);if(h.params=Object.freeze(g.params),h.data=Object.freeze(g.data),this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let _=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,d,h,e),m=await this.applyRedirects.lineralizeSegments(r,_);return this.processSegment(e,i,t,m.concat(f),o,!1,a)}async matchSegmentAgainstRoute(e,t,i,r,s,o){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=await Ud(J1(t,i,r,e,this.urlSerializer,this.abortSignal));if(i.path==="**"&&(t.children={}),!a?.matched)throw new mr(t);e=i._injector??e;let{routes:c}=await this.getChildConfig(e,i,r),l=i._loadedInjector??e,{parameters:u,consumedSegments:d,remainingSegments:f}=a,h=new Wo(d,u,Object.freeze(ae({},this.urlTree.queryParams)),this.urlTree.fragment,nE(i),fi(i),i.component??i._loadedComponent??null,i,iE(i),e),g=jd(h,o,this.paramsInheritanceStrategy);h.params=Object.freeze(g.params),h.data=Object.freeze(g.data);let{segmentGroup:_,slicedSegments:m}=tE(t,d,f,c);if(m.length===0&&_.hasChildren()){let w=await this.processChildren(l,c,_,h);return new Un(h,w)}if(c.length===0&&m.length===0)return new Un(h,[]);let p=fi(i)===s,M=await this.processSegment(l,c,_,m,p?Ve:s,!0,h);return new Un(h,M instanceof Un?[M]:[])}async getChildConfig(e,t,i){if(t.children)return{routes:t.children,injector:e};if(t.loadChildren){if(t._loadedRoutes!==void 0){let s=t._loadedNgModuleFactory;return s&&!t._loadedInjector&&(t._loadedInjector=s.create(e).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Ud(W1(e,t,i,this.urlSerializer,this.abortSignal))){let s=await this.configLoader.loadChildren(e,t);return t._loadedRoutes=s.routes,t._loadedInjector=s.injector,t._loadedNgModuleFactory=s.factory,s}throw X1(t)}return{routes:[],injector:e}}};function oN(n){n.sort((e,t)=>e.value.outlet===Ve?-1:t.value.outlet===Ve?1:e.value.outlet.localeCompare(t.value.outlet))}function aN(n){let e=n.value.routeConfig;return e&&e.path===""}function PE(n){let e=[],t=new Set;for(let i of n){if(!aN(i)){e.push(i);continue}let r=e.find(s=>i.value.routeConfig===s.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):e.push(i)}for(let i of t){let r=PE(i.children);e.push(new Un(i.value,r))}return e.filter(i=>!t.has(i))}function nE(n){return n.data||{}}function iE(n){return n.resolve||{}}function cN(n,e,t,i,r,s,o){return xn(async a=>{let{state:c,tree:l}=await rN(n,e,t,i,a.extractedUrl,r,s,o);return et(ae({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function lN(n){return xn(e=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=e;if(!i.length)return tt(e);let r=new Set(i.map(a=>a.route)),s=new Set;for(let a of r)if(!s.has(a))for(let c of OE(a))s.add(c);let o=0;return Ut(s).pipe(yu(a=>r.has(a)?uN(a,t,n):(a.data=jd(a,a.parent,n).resolve,tt(void 0))),Ei(()=>o++),_u(1),xn(a=>o===s.size?tt(e):sn))})}function OE(n){let e=n.children.map(t=>OE(t)).flat();return[n,...e]}function uN(n,e,t){let i=n.routeConfig,r=n._resolve;return i?.title!==void 0&&!bE(i)&&(r[jc]=i.title),Ga(()=>(n.data=jd(n,n.parent,t).resolve,dN(r,n,e).pipe(Dt(s=>(n._resolvedData=s,n.data=ae(ae({},n.data),s),null)))))}function dN(n,e,t){let i=wv(n);if(i.length===0)return tt({});let r={};return Ut(i).pipe(xn(s=>fN(n[s],e,t).pipe(er(),Ei(o=>{if(o instanceof zc)throw $d(new Hs,o);r[s]=o}))),_u(1),Dt(()=>r),ja(s=>TE(s)?sn:kp(s)))}function fN(n,e,t){let i=e._environmentInjector,r=ea(n,i),s=r.resolve?r.resolve(e,t):dn(i,()=>r(e,t));return Gs(s)}function rE(n){return tr(e=>{let t=n(e);return t?Ut(t).pipe(Dt(()=>e)):tt(e)})}var LE=(()=>{class n{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(s=>s.outlet===Ve);return i}getResolvedTitleForRoute(t){return t.data[jc]}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:()=>ie(hN),providedIn:"root"})}return n})(),hN=(()=>{class n extends LE{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||n)(qe(Yb))};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Kd=new Te("",{factory:()=>({})}),Jd=new Te(""),FE=(()=>{class n{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=ie(ev);async loadComponent(t,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let s=await oE(dn(t,()=>i.loadComponent())),o=await UE(kE(s));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=o,o}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let s=await pN(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=s.routes,i._loadedInjector=s.injector,i._loadedNgModuleFactory=s.factory,s}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();async function pN(n,e,t,i){let r=await oE(dn(t,()=>n.loadChildren())),s=await UE(kE(r)),o;s instanceof fd||Array.isArray(s)?o=s:o=await e.compileModuleAsync(s),i&&i(n);let a,c,l=!1,u;return Array.isArray(o)?(c=o,l=!0):(a=o.create(t).injector,u=o,c=a.get(Jd,[],{optional:!0,self:!0}).flat()),{routes:c.map(Kv),injector:a,factory:u}}function mN(n){return n&&typeof n=="object"&&"default"in n}function kE(n){return mN(n)?n.default:n}async function UE(n){return n}var Jv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:()=>ie(gN),providedIn:"root"})}return n})(),gN=(()=>{class n{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),BE=new Te("");var vN=()=>{},VE=new Te(""),HE=(()=>{class n{currentNavigation=Ni(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=Ni(null);events=new Vt;transitionAbortWithErrorSubject=new Vt;configLoader=ie(FE);environmentInjector=ie(zt);destroyRef=ie(cr);urlSerializer=ie(Xd);rootContexts=ie(Wc);location=ie(zo);inputBindingEnabled=ie(Yd,{optional:!0})!==null;titleStrategy=ie(LE);options=ie(Kd,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=ie(Jv);createViewTransition=ie(BE,{optional:!0});navigationErrorHandler=ie(VE,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>tt(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new Pv(r)),i=r=>this.events.next(new Ov(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;hn(()=>{this.transitions?.next(et(ae({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i}))})}setupNavigations(t){return this.transitions=new rn(null),this.transitions.pipe(Ji(i=>i!==null),tr(i=>{let r=!1,s=new AbortController,o=()=>!r&&this.currentTransition?.id===i.id;return tt(i).pipe(tr(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",En.SupersededByNewNavigation),sn;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?et(ae({},c),{previousNavigation:null}):null,abort:()=>s.abort()});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new Wr(a.id,this.urlSerializer.serialize(a.rawUrl),"",Hd.IgnoredSameUrlNavigation)),a.resolve(!1),sn;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return tt(a).pipe(tr(d=>(this.events.next(new Yo(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),d.id!==this.navigationId?sn:Promise.resolve(d))),cN(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),Ei(d=>{i.targetSnapshot=d.targetSnapshot,i.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation.update(h=>(h.finalUrl=d.urlAfterRedirects,h));let f=new Vc(d.id,this.urlSerializer.serialize(d.extractedUrl),this.urlSerializer.serialize(d.urlAfterRedirects),d.targetSnapshot);this.events.next(f)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:d,extractedUrl:f,source:h,restoredState:g,extras:_}=a,m=new Yo(d,this.urlSerializer.serialize(f),h,g);this.events.next(m);let p=xE(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=et(ae({},a),{targetSnapshot:p,urlAfterRedirects:f,extras:et(ae({},_),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(M=>(M.finalUrl=f,M)),tt(i)}else return this.events.next(new Wr(a.id,this.urlSerializer.serialize(a.extractedUrl),"",Hd.IgnoredByUrlHandlingStrategy)),a.resolve(!1),sn}),Dt(a=>{let c=new Av(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=et(ae({},a),{guards:C1(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),k1(a=>this.events.next(a)),tr(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw $d(this.urlSerializer,a.guardsResult);let c=new Iv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!o())return sn;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",En.GuardRejected),sn;if(a.guards.canActivateChecks.length===0)return tt(a);let l=new Rv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!o())return sn;let u=!1;return tt(a).pipe(lN(this.paramsInheritanceStrategy),Ei({next:()=>{u=!0;let d=new Nv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)},complete:()=>{u||this.cancelNavigationTransition(a,"",En.NoDataFromResolver)}}))}),rE(a=>{let c=u=>{let d=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let f=u._environmentInjector;d.push(this.configLoader.loadComponent(f,u.routeConfig).then(h=>{u.component=h}))}for(let f of u.children)d.push(...c(f));return d},l=c(a.targetSnapshot.root);return l.length===0?tt(a):Ut(Promise.all(l).then(()=>a))}),rE(()=>this.afterPreactivation()),tr(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?Ut(l).pipe(Dt(()=>i)):tt(i)}),Qi(1),Dt(a=>{let c=b1(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=et(ae({},a),{targetRouterState:c}),this.currentNavigation.update(l=>(l.targetRouterState=c,l)),this.events.next(new Ko),o()&&(new jv(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),o()&&(r=!0,this.currentNavigation.update(l=>(l.abort=vN,l)),this.lastSuccessfulNavigation.set(hn(this.currentNavigation)),this.events.next(new jr(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0)))}),Wa(AE(s.signal).pipe(Ji(()=>!r&&!i.targetRouterState),Ei(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",En.Aborted)}))),Ei({complete:()=>{r=!0}}),Wa(this.transitionAbortWithErrorSubject.pipe(Ei(a=>{throw a}))),Vp(()=>{s.abort(),r||this.cancelNavigationTransition(i,"",En.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),ja(a=>{if(r=!0,this.destroyed)return i.resolve(!1),sn;if(DE(a))this.events.next(new hi(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),w1(a)?this.events.next(new Jo(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new Zo(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=dn(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof zc){let{message:u,cancellationCode:d}=$d(this.urlSerializer,l);this.events.next(new hi(i.id,this.urlSerializer.serialize(i.extractedUrl),u,d)),this.events.next(new Jo(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return sn}))}))}cancelNavigationTransition(t,i,r){let s=new hi(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(s),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=hn(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function yN(n){return n!==kc}var zE=new Te("");var _N=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:()=>ie(xN),providedIn:"root"})}return n})(),Yv=class{shouldDetach(e){return!1}store(e,t){}shouldAttach(e){return!1}retrieve(e){return null}shouldReuseRoute(e,t){return e.routeConfig===t.routeConfig}shouldDestroyInjector(e){return!0}},xN=(()=>{class n extends Yv{static \u0275fac=(()=>{let t;return function(r){return(t||(t=Vr(n)))(r||n)}})();static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Qv=(()=>{class n{urlSerializer=ie(Xd);options=ie(Kd,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=ie(zo);urlHandlingStrategy=ie(Jv);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Fi;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let s=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,o=r??s;return o instanceof Fi?this.urlSerializer.serialize(o):o}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=xE(null,ie(zt));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:()=>ie(MN),providedIn:"root"})}return n})(),MN=(()=>{class n extends Qv{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate")})})}handleRouterEvent(t,i){t instanceof Yo?this.updateStateMemento():t instanceof Wr?this.commitTransition(i):t instanceof Vc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Ko?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof hi&&!_E(t)?this.restoreHistory(i):t instanceof Zo?this.restoreHistory(i,!0):t instanceof jr&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,{extras:i,id:r}){let{replaceUrl:s,state:o}=i;if(this.location.isCurrentPathEqualTo(t)||s){let a=this.browserPageId,c=ae(ae({},o),this.generateNgRouterState(r,a));this.location.replaceState(t,"",c)}else{let a=ae(ae({},o),this.generateNgRouterState(r,this.browserPageId+1));this.location.go(t,"",a)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,s=this.currentPageId-r;s!==0?this.location.historyGo(s):this.getCurrentUrlTree()===t.finalUrl&&s===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i){return this.canceledNavigationResolution==="computed"?{navigationId:t,\u0275routerPageId:i}:{navigationId:t}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=Vr(n)))(r||n)}})();static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function GE(n,e){n.events.pipe(Ji(t=>t instanceof jr||t instanceof hi||t instanceof Zo||t instanceof Wr),Dt(t=>t instanceof jr||t instanceof Wr?0:(t instanceof hi?t.code===En.Redirect||t.code===En.SupersededByNewNavigation:!1)?2:1),Ji(t=>t!==2),Qi(1)).subscribe(()=>{e()})}var jE={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},WE={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"},ey=(()=>{class n{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=ie(qg);stateManager=ie(Qv);options=ie(Kd,{optional:!0})||{};pendingTasks=ie(Ur);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=ie(HE);urlSerializer=ie(Xd);location=ie(zo);urlHandlingStrategy=ie(Jv);injector=ie(zt);_events=new Vt;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=ie(_N);injectorCleanup=ie(zE,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=ie(Jd,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!ie(Yd,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new nn;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,s=hn(this.navigationTransitions.currentNavigation);if(r!==null&&s!==null){if(this.stateManager.handleRouterEvent(i,s),i instanceof hi&&i.code!==En.Redirect&&i.code!==En.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof jr)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Jo){let o=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=ae({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||yN(r.source)},o);this.scheduleNavigation(a,kc,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}_1(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),kc,this.stateManager.restoredState())}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r)=>{this.navigateToSyncWithBrowser(t,r,i)})}navigateToSyncWithBrowser(t,i,r){let s={replaceUrl:!0},o=r?.navigationId?r:null;if(r){let c=ae({},r);delete c.navigationId,delete c.\u0275routerPageId,Object.keys(c).length!==0&&(s.state=c)}let a=this.parseUrl(t);this.scheduleNavigation(a,i,o,s).catch(c=>{this.disposed||this.injector.get(Ri)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return hn(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(Kv),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:s,fragment:o,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:o,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=ae(ae({},this.currentUrlTree.queryParams),s);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=s||null}u!==null&&(u=this.removeEmptyProps(u));let d;try{let f=r?r.snapshot:this.routerState.snapshot.root;d=mE(f)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),d=this.currentUrlTree.root}return gE(d,t,u,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=Xo(t)?t:this.parseUrl(t),s=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(s,kc,null,i)}navigate(t,i={skipLocationChange:!1}){return bN(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.console.warn(Ka(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=ae({},jE):i===!1?r=ae({},WE):r=i,Xo(t))return Zb(this.currentUrlTree,t,r);let s=this.parseUrl(t);return Zb(this.currentUrlTree,s,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,s])=>(s!=null&&(i[r]=s),i),{})}scheduleNavigation(t,i,r,s,o){if(this.disposed)return Promise.resolve(!1);let a,c,l;o?(a=o.resolve,c=o.reject,l=o.promise):l=new Promise((d,f)=>{a=d,c=f});let u=this.pendingTasks.add();return GE(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:s,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Oe({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function bN(n){for(let e=0;e<n.length;e++)if(n[e]==null)throw new Ce(4008,!1)}var EN=new Te("");function ty(n,...e){return So([{provide:Jd,multi:!0,useValue:n},[],{provide:zs,useFactory:SN},{provide:pd,multi:!0,useFactory:wN},e.map(t=>t.\u0275providers)])}function SN(){return ie(ey).routerState.root}function wN(){let n=ie(ai);return e=>{let t=n.get(Ls);if(e!==t.components[0])return;let i=n.get(ey),r=n.get(CN);n.get(DN)===1&&i.initialNavigation(),n.get(TN,null,{optional:!0})?.setUpPreloading(),n.get(EN,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var CN=new Te("",{factory:()=>new Vt}),DN=new Te("",{factory:()=>1});var TN=new Te("");var $E=[];var qE={providers:[jm(),ty($E)]};var tS=(()=>{class n{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,i){this._renderer=t,this._elementRef=i}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(i){return new(i||n)(Tt(bc),Tt(ur))};static \u0275dir=kn({type:n})}return n})(),ay=(()=>{class n extends tS{static \u0275fac=(()=>{let t;return function(r){return(t||(t=Vr(n)))(r||n)}})();static \u0275dir=kn({type:n,features:[hr]})}return n})(),rf=new Te("");var IN={provide:rf,useExisting:sr(()=>sf),multi:!0};function RN(){let n=Qn()?Qn().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var NN=new Te(""),sf=(()=>{class n extends tS{_compositionMode;_composing=!1;constructor(t,i,r){super(t,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!RN())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(i){return new(i||n)(Tt(bc),Tt(ur),Tt(NN,8))};static \u0275dir=kn({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&Oi("input",function(o){return r._handleInput(o.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(o){return r._compositionEnd(o.target.value)})},standalone:!1,features:[Vo([IN]),hr]})}return n})();var PN=new Te(""),ON=new Te("");function nS(n){return n!=null}function iS(n){return Os(n)?Ut(n):n}function rS(n){let e={};return n.forEach(t=>{e=t!=null?ae(ae({},e),t):e}),Object.keys(e).length===0?null:e}function sS(n,e){return e.map(t=>t(n))}function LN(n){return!n.validate}function oS(n){return n.map(e=>LN(e)?e:t=>e.validate(t))}function FN(n){if(!n)return null;let e=n.filter(nS);return e.length==0?null:function(t){return rS(sS(t,e))}}function aS(n){return n!=null?FN(oS(n)):null}function kN(n){if(!n)return null;let e=n.filter(nS);return e.length==0?null:function(t){let i=sS(t,e).map(iS);return Bp(i).pipe(Dt(rS))}}function cS(n){return n!=null?kN(oS(n)):null}function XE(n,e){return n===null?[e]:Array.isArray(n)?[...n,e]:[n,e]}function UN(n){return n._rawValidators}function BN(n){return n._rawAsyncValidators}function ny(n){return n?Array.isArray(n)?n:[n]:[]}function ef(n,e){return Array.isArray(n)?n.includes(e):n===e}function YE(n,e){let t=ny(e);return ny(n).forEach(r=>{ef(t,r)||t.push(r)}),t}function ZE(n,e){return ny(e).filter(t=>!ef(n,t))}var tf=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(e){this._rawValidators=e||[],this._composedValidatorFn=aS(this._rawValidators)}_setAsyncValidators(e){this._rawAsyncValidators=e||[],this._composedAsyncValidatorFn=cS(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(e){this._onDestroyCallbacks.push(e)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(e=>e()),this._onDestroyCallbacks=[]}reset(e=void 0){this.control?.reset(e)}hasError(e,t){return this.control?this.control.hasError(e,t):!1}getError(e,t){return this.control?this.control.getError(e,t):null}},iy=class extends tf{name;get formDirective(){return null}get path(){return null}},Kc=class extends tf{_parent=null;name=null;valueAccessor=null},ry=class{_cd;constructor(e){this._cd=e}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var lS=(()=>{class n extends ry{constructor(t){super(t)}static \u0275fac=function(i){return new(i||n)(Tt(Kc,2))};static \u0275dir=kn({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&_d("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[hr]})}return n})();var qc="VALID",Qd="INVALID",ta="PENDING",Xc="DISABLED",js=class{},nf=class extends js{value;source;constructor(e,t){super(),this.value=e,this.source=t}},Yc=class extends js{pristine;source;constructor(e,t){super(),this.pristine=e,this.source=t}},Zc=class extends js{touched;source;constructor(e,t){super(),this.touched=e,this.source=t}},na=class extends js{status;source;constructor(e,t){super(),this.status=e,this.source=t}};var sy=class extends js{source;constructor(e){super(),this.source=e}};function VN(n){return(of(n)?n.validators:n)||null}function HN(n){return Array.isArray(n)?aS(n):n||null}function zN(n,e){return(of(e)?e.asyncValidators:n)||null}function GN(n){return Array.isArray(n)?cS(n):n||null}function of(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}var oy=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(e,t){this._assignValidators(e),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(e){this._rawValidators=this._composedValidatorFn=e}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(e){this._rawAsyncValidators=this._composedAsyncValidatorFn=e}get parent(){return this._parent}get status(){return hn(this.statusReactive)}set status(e){hn(()=>this.statusReactive.set(e))}_status=Ho(()=>this.statusReactive());statusReactive=Ni(void 0);get valid(){return this.status===qc}get invalid(){return this.status===Qd}get pending(){return this.status==ta}get disabled(){return this.status===Xc}get enabled(){return this.status!==Xc}errors;get pristine(){return hn(this.pristineReactive)}set pristine(e){hn(()=>this.pristineReactive.set(e))}_pristine=Ho(()=>this.pristineReactive());pristineReactive=Ni(!0);get dirty(){return!this.pristine}get touched(){return hn(this.touchedReactive)}set touched(e){hn(()=>this.touchedReactive.set(e))}_touched=Ho(()=>this.touchedReactive());touchedReactive=Ni(!1);get untouched(){return!this.touched}_events=new Vt;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(e){this._assignValidators(e)}setAsyncValidators(e){this._assignAsyncValidators(e)}addValidators(e){this.setValidators(YE(e,this._rawValidators))}addAsyncValidators(e){this.setAsyncValidators(YE(e,this._rawAsyncValidators))}removeValidators(e){this.setValidators(ZE(e,this._rawValidators))}removeAsyncValidators(e){this.setAsyncValidators(ZE(e,this._rawAsyncValidators))}hasValidator(e){return ef(this._rawValidators,e)}hasAsyncValidator(e){return ef(this._rawAsyncValidators,e)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(e={}){let t=this.touched===!1;this.touched=!0;let i=e.sourceControl??this;e.onlySelf||this._parent?.markAsTouched(et(ae({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Zc(!0,i))}markAllAsDirty(e={}){this.markAsDirty({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsDirty(e))}markAllAsTouched(e={}){this.markAsTouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(e))}markAsUntouched(e={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:i})}),e.onlySelf||this._parent?._updateTouched(e,i),t&&e.emitEvent!==!1&&this._events.next(new Zc(!1,i))}markAsDirty(e={}){let t=this.pristine===!0;this.pristine=!1;let i=e.sourceControl??this;e.onlySelf||this._parent?.markAsDirty(et(ae({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Yc(!1,i))}markAsPristine(e={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:e.emitEvent})}),e.onlySelf||this._parent?._updatePristine(e,i),t&&e.emitEvent!==!1&&this._events.next(new Yc(!0,i))}markAsPending(e={}){this.status=ta;let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new na(this.status,t)),this.statusChanges.emit(this.status)),e.onlySelf||this._parent?.markAsPending(et(ae({},e),{sourceControl:t}))}disable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Xc,this.errors=null,this._forEachChild(r=>{r.disable(et(ae({},e),{onlySelf:!0}))}),this._updateValue();let i=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new nf(this.value,i)),this._events.next(new na(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(et(ae({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=qc,this._forEachChild(i=>{i.enable(et(ae({},e),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent}),this._updateAncestors(et(ae({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(e,t){e.onlySelf||(this._parent?.updateValueAndValidity(e),e.skipPristineCheck||this._parent?._updatePristine({},t),this._parent?._updateTouched({},t))}setParent(e){this._parent=e}getRawValue(){return this.value}updateValueAndValidity(e={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===qc||this.status===ta)&&this._runAsyncValidator(i,e.emitEvent)}let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new nf(this.value,t)),this._events.next(new na(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),e.onlySelf||this._parent?.updateValueAndValidity(et(ae({},e),{sourceControl:t}))}_updateTreeValidity(e={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(e)),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Xc:qc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(e,t){if(this.asyncValidator){this.status=ta,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1,shouldHaveEmitted:e!==!1};let i=iS(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:e})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let e=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,e}return!1}setErrors(e,t={}){this.errors=e,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(e){let t=e;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(e,t){let i=t?this.get(t):this;return i?.errors?i.errors[e]:null}hasError(e,t){return!!this.getError(e,t)}get root(){let e=this;for(;e._parent;)e=e._parent;return e}_updateControlsErrors(e,t,i){this.status=this._calculateStatus(),e&&this.statusChanges.emit(this.status),(e||i)&&this._events.next(new na(this.status,t)),this._parent&&this._parent._updateControlsErrors(e,t,i)}_initObservables(){this.valueChanges=new Ht,this.statusChanges=new Ht}_calculateStatus(){return this._allControlsDisabled()?Xc:this.errors?Qd:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ta)?ta:this._anyControlsHaveStatus(Qd)?Qd:qc}_anyControlsHaveStatus(e){return this._anyControls(t=>t.status===e)}_anyControlsDirty(){return this._anyControls(e=>e.dirty)}_anyControlsTouched(){return this._anyControls(e=>e.touched)}_updatePristine(e,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,e.onlySelf||this._parent?._updatePristine(e,t),r&&this._events.next(new Yc(this.pristine,t))}_updateTouched(e={},t){this.touched=this._anyControlsTouched(),this._events.next(new Zc(this.touched,t)),e.onlySelf||this._parent?._updateTouched(e,t)}_onDisabledChange=[];_registerOnCollectionChange(e){this._onCollectionChange=e}_setUpdateStrategy(e){of(e)&&e.updateOn!=null&&(this._updateOn=e.updateOn)}_parentMarkedDirty(e){return!e&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(e){return null}_assignValidators(e){this._rawValidators=Array.isArray(e)?e.slice():e,this._composedValidatorFn=HN(this._rawValidators)}_assignAsyncValidators(e){this._rawAsyncValidators=Array.isArray(e)?e.slice():e,this._composedAsyncValidatorFn=GN(this._rawAsyncValidators)}};var uS=new Te("",{factory:()=>cy}),cy="always";function jN(n,e){return[...e.path,n]}function WN(n,e,t=cy){qN(n,e),e.valueAccessor.writeValue(n.value),(n.disabled||t==="always")&&e.valueAccessor.setDisabledState?.(n.disabled),XN(n,e),ZN(n,e),YN(n,e),$N(n,e)}function KE(n,e){n.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(e)})}function $N(n,e){if(e.valueAccessor.setDisabledState){let t=i=>{e.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(t),e._registerOnDestroy(()=>{n._unregisterOnDisabledChange(t)})}}function qN(n,e){let t=UN(n);e.validator!==null?n.setValidators(XE(t,e.validator)):typeof t=="function"&&n.setValidators([t]);let i=BN(n);e.asyncValidator!==null?n.setAsyncValidators(XE(i,e.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();KE(e._rawValidators,r),KE(e._rawAsyncValidators,r)}function XN(n,e){e.valueAccessor.registerOnChange(t=>{n._pendingValue=t,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&dS(n,e)})}function YN(n,e){e.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&dS(n,e),n.updateOn!=="submit"&&n.markAsTouched()})}function dS(n,e){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),e.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function ZN(n,e){let t=(i,r)=>{e.valueAccessor.writeValue(i),r&&e.viewToModelUpdate(i)};n.registerOnChange(t),e._registerOnDestroy(()=>{n._unregisterOnChange(t)})}function KN(n,e){if(!n.hasOwnProperty("model"))return!1;let t=n.model;return t.isFirstChange()?!0:!Object.is(e,t.currentValue)}function JN(n){return Object.getPrototypeOf(n.constructor)===ay}function QN(n,e){if(!e)return null;Array.isArray(e);let t,i,r;return e.forEach(s=>{s.constructor===sf?t=s:JN(s)?i=s:r=s}),r||i||t||null}function JE(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function QE(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var eP=class extends oy{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(e=null,t,i){super(VN(t),zN(i,t)),this._applyFormState(e),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),of(t)&&(t.nonNullable||t.initialValueIsDefault)&&(QE(e)?this.defaultValue=e.value:this.defaultValue=e)}setValue(e,t={}){this.value=this._pendingValue=e,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(e,t={}){this.setValue(e,t)}reset(e=this.defaultValue,t={}){this._applyFormState(e),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),t.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,t?.emitEvent!==!1&&this._events.next(new sy(this))}_updateValue(){}_anyControls(e){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(e){this._onChange.push(e)}_unregisterOnChange(e){JE(this._onChange,e)}registerOnDisabledChange(e){this._onDisabledChange.push(e)}_unregisterOnDisabledChange(e){JE(this._onDisabledChange,e)}_forEachChild(e){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(e){QE(e)?(this.value=this._pendingValue=e.value,e.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=e}};var tP={provide:Kc,useExisting:sr(()=>ly)},eS=Promise.resolve(),ly=(()=>{class n extends Kc{_changeDetectorRef;callSetDisabledState;control=new eP;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new Ht;constructor(t,i,r,s,o,a){super(),this._changeDetectorRef=o,this.callSetDisabledState=a,this._parent=t,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=QN(this,s)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),KN(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){WN(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){eS.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&lv(i);eS.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?jN(t,this._parent):[t]}static \u0275fac=function(i){return new(i||n)(Tt(iy,9),Tt(PN,10),Tt(ON,10),Tt(rf,10),Tt(Us,8),Tt(uS,8))};static \u0275dir=kn({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Vo([tP]),hr,Fo]})}return n})();var nP={provide:rf,useExisting:sr(()=>uy),multi:!0},uy=(()=>{class n extends ay{writeValue(t){let i=t??"";this.setProperty("value",i)}registerOnChange(t){this.onChange=i=>{t(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=Vr(n)))(r||n)}})();static \u0275dir=kn({type:n,selectors:[["input","type","number","formControlName",""],["input","type","number","formControl",""],["input","type","number","ngModel",""]],hostBindings:function(i,r){i&1&&Oi("input",function(o){return r.onChange(o.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Vo([nP]),hr]})}return n})();var iP={provide:rf,useExisting:sr(()=>dy),multi:!0},dy=(()=>{class n extends ay{writeValue(t){this.setProperty("value",parseFloat(t))}registerOnChange(t){this.onChange=i=>{t(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=Vr(n)))(r||n)}})();static \u0275dir=kn({type:n,selectors:[["input","type","range","formControlName",""],["input","type","range","formControl",""],["input","type","range","ngModel",""]],hostBindings:function(i,r){i&1&&Oi("change",function(o){return r.onChange(o.target.value)})("input",function(o){return r.onChange(o.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Vo([iP]),hr]})}return n})();var rP=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=zr({type:n});static \u0275inj=or({})}return n})();var fS=(()=>{class n{static withConfig(t){return{ngModule:n,providers:[{provide:uS,useValue:t.callSetDisabledState??cy}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=zr({type:n});static \u0275inj=or({imports:[rP]})}return n})();var ss={LEFT:0,MIDDLE:1,RIGHT:2,ROTATE:0,DOLLY:1,PAN:2},os={ROTATE:0,PAN:1,DOLLY_PAN:2,DOLLY_ROTATE:3},kS=0,zy=1,US=2;var Rl=1,BS=2,Ia=3,Sr=0,Cn=1,Gi=2,ji=0,Zs=1,Gy=2,jy=3,Wy=4,VS=5;var Jr=100,HS=101,zS=102,GS=103,jS=104,WS=200,$S=201,qS=202,XS=203,Pf=204,Of=205,YS=206,ZS=207,KS=208,JS=209,QS=210,ew=211,tw=212,nw=213,iw=214,Lf=0,Ff=1,kf=2,Ks=3,Uf=4,Bf=5,Vf=6,Hf=7,$y=0,rw=1,sw=2,_i=0,qy=1,Xy=2,Yy=3,Zy=4,Ky=5,Jy=6,Qy=7;var Ly=300,as=301,ro=302,mh=303,gh=304,Nl=306,zf=1e3,Ui=1001,Gf=1002,en=1003,ow=1004;var Pl=1005;var cn=1006,vh=1007;var cs=1008;var $n=1009,e_=1010,t_=1011,Ra=1012,yh=1013,xi=1014,Mi=1015,Wi=1016,_h=1017,xh=1018,Na=1020,n_=35902,i_=35899,r_=1021,s_=1022,ii=1023,Vi=1026,ls=1027,o_=1028,Mh=1029,so=1030,bh=1031;var Eh=1033,Ol=33776,Ll=33777,Fl=33778,kl=33779,Sh=35840,wh=35841,Ch=35842,Dh=35843,Th=36196,Ah=37492,Ih=37496,Rh=37488,Nh=37489,Ph=37490,Oh=37491,Lh=37808,Fh=37809,kh=37810,Uh=37811,Bh=37812,Vh=37813,Hh=37814,zh=37815,Gh=37816,jh=37817,Wh=37818,$h=37819,qh=37820,Xh=37821,Yh=36492,Zh=36494,Kh=36495,Jh=36283,Qh=36284,ep=36285,tp=36286;var cl=2300,jf=2301,Nf=2302,Fy=2303,ky=2400,Uy=2401,By=2402;var aw=3200;var a_=0,cw=1,wr="",Hn="srgb",Js="srgb-linear",ll="linear",ft="srgb";var Ys=7680;var Vy=519,lw=512,uw=513,dw=514,np=515,fw=516,hw=517,ip=518,pw=519,Wf=35044;var c_="300 es",yi=2e3,ul=2001;function oP(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function aP(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function dl(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function mw(){let n=dl("canvas");return n.style.display="block",n}var hS={},xa=null;function fl(...n){let e="THREE."+n.shift();xa?xa("log",e,...n):console.log(e,...n)}function gw(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Ae(...n){n=gw(n);let e="THREE."+n.shift();if(xa)xa("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Re(...n){n=gw(n);let e="THREE."+n.shift();if(xa)xa("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function hl(...n){let e=n.join(" ");e in hS||(hS[e]=!0,Ae(...n))}function vw(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var yw={[Lf]:Ff,[kf]:Vf,[Uf]:Hf,[Ks]:Bf,[Ff]:Lf,[Vf]:kf,[Hf]:Uf,[Bf]:Ks},Hi=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},pn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],pS=1234567,ol=Math.PI/180,Ma=180/Math.PI;function br(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(pn[n&255]+pn[n>>8&255]+pn[n>>16&255]+pn[n>>24&255]+"-"+pn[e&255]+pn[e>>8&255]+"-"+pn[e>>16&15|64]+pn[e>>24&255]+"-"+pn[t&63|128]+pn[t>>8&255]+"-"+pn[t>>16&255]+pn[t>>24&255]+pn[i&255]+pn[i>>8&255]+pn[i>>16&255]+pn[i>>24&255]).toLowerCase()}function Qe(n,e,t){return Math.max(e,Math.min(t,n))}function l_(n,e){return(n%e+e)%e}function cP(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function lP(n,e,t){return n!==e?(t-n)/(e-n):0}function al(n,e,t){return(1-t)*n+t*e}function uP(n,e,t,i){return al(n,e,1-Math.exp(-t*i))}function dP(n,e=1){return e-Math.abs(l_(n,e*2)-e)}function fP(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function hP(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function pP(n,e){return n+Math.floor(Math.random()*(e-n+1))}function mP(n,e){return n+Math.random()*(e-n)}function gP(n){return n*(.5-Math.random())}function vP(n){n!==void 0&&(pS=n);let e=pS+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function yP(n){return n*ol}function _P(n){return n*Ma}function xP(n){return(n&n-1)===0&&n!==0}function MP(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function bP(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function EP(n,e,t,i,r){let s=Math.cos,o=Math.sin,a=s(t/2),c=o(t/2),l=s((e+i)/2),u=o((e+i)/2),d=s((e-i)/2),f=o((e-i)/2),h=s((i-e)/2),g=o((i-e)/2);switch(r){case"XYX":n.set(a*u,c*d,c*f,a*l);break;case"YZY":n.set(c*f,a*u,c*d,a*l);break;case"ZXZ":n.set(c*d,c*f,a*u,a*l);break;case"XZX":n.set(a*u,c*g,c*h,a*l);break;case"YXY":n.set(c*h,a*u,c*g,a*l);break;case"ZYZ":n.set(c*g,c*h,a*u,a*l);break;default:Ae("MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function vi(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function mt(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var u_={DEG2RAD:ol,RAD2DEG:Ma,generateUUID:br,clamp:Qe,euclideanModulo:l_,mapLinear:cP,inverseLerp:lP,lerp:al,damp:uP,pingpong:dP,smoothstep:fP,smootherstep:hP,randInt:pP,randFloat:mP,randFloatSpread:gP,seededRandom:vP,degToRad:yP,radToDeg:_P,isPowerOfTwo:xP,ceilPowerOfTwo:MP,floorPowerOfTwo:bP,setQuaternionFromProperEuler:EP,normalize:mt,denormalize:vi},Ne=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Qe(this.x,e.x,t.x),this.y=Qe(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=Qe(this.x,e,t),this.y=Qe(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Qe(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Qe(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},zn=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],f=s[o+0],h=s[o+1],g=s[o+2],_=s[o+3];if(d!==_||c!==f||l!==h||u!==g){let m=c*f+l*h+u*g+d*_;m<0&&(f=-f,h=-h,g=-g,_=-_,m=-m);let p=1-a;if(m<.9995){let M=Math.acos(m),w=Math.sin(M);p=Math.sin(p*M)/w,a=Math.sin(a*M)/w,c=c*p+f*a,l=l*p+h*a,u=u*p+g*a,d=d*p+_*a}else{c=c*p+f*a,l=l*p+h*a,u=u*p+g*a,d=d*p+_*a;let M=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=M,l*=M,u*=M,d*=M}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=s[o],f=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*d+c*h-l*f,e[t+1]=c*g+u*f+l*d-a*h,e[t+2]=l*g+u*h+a*f-c*d,e[t+3]=u*g-a*d-c*f-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(s/2),f=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"YXZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"ZXY":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"ZYX":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"YZX":this._x=f*u*d+l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d-f*h*g;break;case"XZY":this._x=f*u*d-l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d+f*h*g;break;default:Ae("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(Qe(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},P=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(mS.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(mS.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+c*l+o*d-a*u,this.y=i+c*u+a*l-s*d,this.z=r+c*d+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Qe(this.x,e.x,t.x),this.y=Qe(this.y,e.y,t.y),this.z=Qe(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=Qe(this.x,e,t),this.y=Qe(this.y,e,t),this.z=Qe(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Qe(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return fy.copy(this).projectOnVector(e),this.sub(fy)}reflect(e){return this.sub(fy.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Qe(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},fy=new P,mS=new zn,ze=class n{constructor(e,t,i,r,s,o,a,c,l){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],f=i[2],h=i[5],g=i[8],_=r[0],m=r[3],p=r[6],M=r[1],w=r[4],S=r[7],T=r[2],D=r[5],I=r[8];return s[0]=o*_+a*M+c*T,s[3]=o*m+a*w+c*D,s[6]=o*p+a*S+c*I,s[1]=l*_+u*M+d*T,s[4]=l*m+u*w+d*D,s[7]=l*p+u*S+d*I,s[2]=f*_+h*M+g*T,s[5]=f*m+h*w+g*D,s[8]=f*p+h*S+g*I,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*o-a*l,f=a*c-u*s,h=l*s-o*c,g=t*d+i*f+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let _=1/g;return e[0]=d*_,e[1]=(r*l-u*i)*_,e[2]=(a*i-r*o)*_,e[3]=f*_,e[4]=(u*t-r*c)*_,e[5]=(r*s-a*t)*_,e[6]=h*_,e[7]=(i*c-l*t)*_,e[8]=(o*t-i*s)*_,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(hy.makeScale(e,t)),this}rotate(e){return this.premultiply(hy.makeRotation(-e)),this}translate(e,t){return this.premultiply(hy.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},hy=new ze,gS=new ze().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),vS=new ze().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function SP(){let n={enabled:!0,workingColorSpace:Js,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===ft&&(r.r=Er(r.r),r.g=Er(r.g),r.b=Er(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===ft&&(r.r=_a(r.r),r.g=_a(r.g),r.b=_a(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===wr?ll:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return hl("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return hl("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Js]:{primaries:e,whitePoint:i,transfer:ll,toXYZ:gS,fromXYZ:vS,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Hn},outputColorSpaceConfig:{drawingBufferColorSpace:Hn}},[Hn]:{primaries:e,whitePoint:i,transfer:ft,toXYZ:gS,fromXYZ:vS,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Hn}}}),n}var nt=SP();function Er(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function _a(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var ia,$f=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{ia===void 0&&(ia=dl("canvas")),ia.width=e.width,ia.height=e.height;let r=ia.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=ia}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=dl("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Er(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Er(t[i]/255)*255):t[i]=Er(t[i]);return{data:t,width:e.width,height:e.height}}else return Ae("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},wP=0,ba=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:wP++}),this.uuid=br(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayHeight,t.displayWidth,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(py(r[o].image)):s.push(py(r[o]))}else s=py(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function py(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?$f.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Ae("Texture: Unable to serialize Texture."),{})}var CP=0,my=new P,$i=(()=>{class n extends Hi{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Ui,s=Ui,o=cn,a=cs,c=ii,l=$n,u=n.DEFAULT_ANISOTROPY,d=wr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:CP++}),this.uuid=br(),this.name="",this.source=new ba(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new Ne(0,0),this.repeat=new Ne(1,1),this.center=new Ne(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new ze,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0}get width(){return this.source.getSize(my).x}get height(){return this.source.getSize(my).y}get depth(){return this.source.getSize(my).z}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Ae(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){Ae(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==Ly)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case zf:t.x=t.x-Math.floor(t.x);break;case Ui:t.x=t.x<0?0:1;break;case Gf:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case zf:t.y=t.y-Math.floor(t.y);break;case Ui:t.y=t.y<0?0:1;break;case Gf:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=Ly,n.DEFAULT_ANISOTROPY=1,n})(),Nt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],d=c[8],f=c[1],h=c[5],g=c[9],_=c[2],m=c[6],p=c[10];if(Math.abs(u-f)<.01&&Math.abs(d-_)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+_)<.1&&Math.abs(g+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let w=(l+1)/2,S=(h+1)/2,T=(p+1)/2,D=(u+f)/4,I=(d+_)/4,y=(g+m)/4;return w>S&&w>T?w<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(w),r=D/i,s=I/i):S>T?S<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(S),i=D/r,s=y/r):T<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(T),i=I/s,r=y/s),this.set(i,r,s,t),this}let M=Math.sqrt((m-g)*(m-g)+(d-_)*(d-_)+(f-u)*(f-u));return Math.abs(M)<.001&&(M=1),this.x=(m-g)/M,this.y=(d-_)/M,this.z=(f-u)/M,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Qe(this.x,e.x,t.x),this.y=Qe(this.y,e.y,t.y),this.z=Qe(this.z,e.z,t.z),this.w=Qe(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=Qe(this.x,e,t),this.y=Qe(this.y,e,t),this.z=Qe(this.z,e,t),this.w=Qe(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Qe(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},qf=class extends Hi{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:cn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Nt(0,0,e,t),this.scissorTest=!1,this.viewport=new Nt(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new $i(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:cn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new ba(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},Gn=class extends qf{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},pl=class extends $i{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=en,this.minFilter=en,this.wrapR=Ui,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Xf=class extends $i{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=en,this.minFilter=en,this.wrapR=Ui,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Rt=class n{constructor(e,t,i,r,s,o,a,c,l,u,d,f,h,g,_,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,_,m)}set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,_,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=d,p[14]=f,p[3]=h,p[7]=g,p[11]=_,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/ra.setFromMatrixColumn(e,0).length(),s=1/ra.setFromMatrixColumn(e,1).length(),o=1/ra.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let f=o*u,h=o*d,g=a*u,_=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=h+g*l,t[5]=f-_*l,t[9]=-a*c,t[2]=_-f*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let f=c*u,h=c*d,g=l*u,_=l*d;t[0]=f+_*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=_+f*a,t[10]=o*c}else if(e.order==="ZXY"){let f=c*u,h=c*d,g=l*u,_=l*d;t[0]=f-_*a,t[4]=-o*d,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=_-f*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let f=o*u,h=o*d,g=a*u,_=a*d;t[0]=c*u,t[4]=g*l-h,t[8]=f*l+_,t[1]=c*d,t[5]=_*l+f,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let f=o*c,h=o*l,g=a*c,_=a*l;t[0]=c*u,t[4]=_-f*d,t[8]=g*d+h,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*d+g,t[10]=f-_*d}else if(e.order==="XZY"){let f=o*c,h=o*l,g=a*c,_=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=f*d+_,t[5]=o*u,t[9]=h*d-g,t[2]=g*d-h,t[6]=a*u,t[10]=_*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(DP,e,TP)}lookAt(e,t,i){let r=this.elements;return Bn.subVectors(e,t),Bn.lengthSq()===0&&(Bn.z=1),Bn.normalize(),$r.crossVectors(i,Bn),$r.lengthSq()===0&&(Math.abs(i.z)===1?Bn.x+=1e-4:Bn.z+=1e-4,Bn.normalize(),$r.crossVectors(i,Bn)),$r.normalize(),af.crossVectors(Bn,$r),r[0]=$r.x,r[4]=af.x,r[8]=Bn.x,r[1]=$r.y,r[5]=af.y,r[9]=Bn.y,r[2]=$r.z,r[6]=af.z,r[10]=Bn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],f=i[9],h=i[13],g=i[2],_=i[6],m=i[10],p=i[14],M=i[3],w=i[7],S=i[11],T=i[15],D=r[0],I=r[4],y=r[8],E=r[12],q=r[1],C=r[5],U=r[9],V=r[13],j=r[2],B=r[6],H=r[10],L=r[14],Q=r[3],Z=r[7],de=r[11],ge=r[15];return s[0]=o*D+a*q+c*j+l*Q,s[4]=o*I+a*C+c*B+l*Z,s[8]=o*y+a*U+c*H+l*de,s[12]=o*E+a*V+c*L+l*ge,s[1]=u*D+d*q+f*j+h*Q,s[5]=u*I+d*C+f*B+h*Z,s[9]=u*y+d*U+f*H+h*de,s[13]=u*E+d*V+f*L+h*ge,s[2]=g*D+_*q+m*j+p*Q,s[6]=g*I+_*C+m*B+p*Z,s[10]=g*y+_*U+m*H+p*de,s[14]=g*E+_*V+m*L+p*ge,s[3]=M*D+w*q+S*j+T*Q,s[7]=M*I+w*C+S*B+T*Z,s[11]=M*y+w*U+S*H+T*de,s[15]=M*E+w*V+S*L+T*ge,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],f=e[10],h=e[14],g=e[3],_=e[7],m=e[11],p=e[15],M=c*h-l*f,w=a*h-l*d,S=a*f-c*d,T=o*h-l*u,D=o*f-c*u,I=o*d-a*u;return t*(_*M-m*w+p*S)-i*(g*M-m*T+p*D)+r*(g*w-_*T+p*I)-s*(g*S-_*D+m*I)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],f=e[10],h=e[11],g=e[12],_=e[13],m=e[14],p=e[15],M=t*a-i*o,w=t*c-r*o,S=t*l-s*o,T=i*c-r*a,D=i*l-s*a,I=r*l-s*c,y=u*_-d*g,E=u*m-f*g,q=u*p-h*g,C=d*m-f*_,U=d*p-h*_,V=f*p-h*m,j=M*V-w*U+S*C+T*q-D*E+I*y;if(j===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let B=1/j;return e[0]=(a*V-c*U+l*C)*B,e[1]=(r*U-i*V-s*C)*B,e[2]=(_*I-m*D+p*T)*B,e[3]=(f*D-d*I-h*T)*B,e[4]=(c*q-o*V-l*E)*B,e[5]=(t*V-r*q+s*E)*B,e[6]=(m*S-g*I-p*w)*B,e[7]=(u*I-f*S+h*w)*B,e[8]=(o*U-a*q+l*y)*B,e[9]=(i*q-t*U-s*y)*B,e[10]=(g*D-_*S+p*M)*B,e[11]=(d*S-u*D-h*M)*B,e[12]=(a*E-o*C-c*y)*B,e[13]=(t*C-i*E+r*y)*B,e[14]=(_*w-g*T-m*M)*B,e[15]=(u*T-d*w+f*M)*B,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,d=a+a,f=s*l,h=s*u,g=s*d,_=o*u,m=o*d,p=a*d,M=c*l,w=c*u,S=c*d,T=i.x,D=i.y,I=i.z;return r[0]=(1-(_+p))*T,r[1]=(h+S)*T,r[2]=(g-w)*T,r[3]=0,r[4]=(h-S)*D,r[5]=(1-(f+p))*D,r[6]=(m+M)*D,r[7]=0,r[8]=(g+w)*I,r[9]=(m-M)*I,r[10]=(1-(f+_))*I,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=ra.set(r[0],r[1],r[2]).length(),a=ra.set(r[4],r[5],r[6]).length(),c=ra.set(r[8],r[9],r[10]).length();s<0&&(o=-o),pi.copy(this);let l=1/o,u=1/a,d=1/c;return pi.elements[0]*=l,pi.elements[1]*=l,pi.elements[2]*=l,pi.elements[4]*=u,pi.elements[5]*=u,pi.elements[6]*=u,pi.elements[8]*=d,pi.elements[9]*=d,pi.elements[10]*=d,t.setFromRotationMatrix(pi),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=yi,c=!1){let l=this.elements,u=2*s/(t-e),d=2*s/(i-r),f=(t+e)/(t-e),h=(i+r)/(i-r),g,_;if(c)g=s/(o-s),_=o*s/(o-s);else if(a===yi)g=-(o+s)/(o-s),_=-2*o*s/(o-s);else if(a===ul)g=-o/(o-s),_=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=f,l[12]=0,l[1]=0,l[5]=d,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=_,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=yi,c=!1){let l=this.elements,u=2/(t-e),d=2/(i-r),f=-(t+e)/(t-e),h=-(i+r)/(i-r),g,_;if(c)g=1/(o-s),_=o/(o-s);else if(a===yi)g=-2/(o-s),_=-(o+s)/(o-s);else if(a===ul)g=-1/(o-s),_=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=f,l[1]=0,l[5]=d,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=g,l[14]=_,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},ra=new P,pi=new Rt,DP=new P(0,0,0),TP=new P(1,1,1),$r=new P,af=new P,Bn=new P,yS=new Rt,_S=new zn,Qr=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],d=s[9],f=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(Qe(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-Qe(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-f,o),this._z=0);break;case"ZXY":this._x=Math.asin(Qe(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-f,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-Qe(f,-1,1)),Math.abs(f)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(Qe(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-f,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-Qe(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:Ae("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return yS.makeRotationFromQuaternion(t),this.setFromRotationMatrix(yS,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return _S.setFromEuler(this),this.setFromQuaternion(_S,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),ml=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},AP=0,xS=new P,sa=new zn,gr=new Rt,cf=new P,Jc=new P,IP=new P,RP=new zn,MS=new P(1,0,0),bS=new P(0,1,0),ES=new P(0,0,1),SS={type:"added"},NP={type:"removed"},oa={type:"childadded",child:null},gy={type:"childremoved",child:null},qi=(()=>{class n extends Hi{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:AP++}),this.uuid=br(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new P,i=new Qr,r=new zn,s=new P(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Rt},normalMatrix:{value:new ze}}),this.matrix=new Rt,this.matrixWorld=new Rt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new ml,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return sa.setFromAxisAngle(t,i),this.quaternion.multiply(sa),this}rotateOnWorldAxis(t,i){return sa.setFromAxisAngle(t,i),this.quaternion.premultiply(sa),this}rotateX(t){return this.rotateOnAxis(MS,t)}rotateY(t){return this.rotateOnAxis(bS,t)}rotateZ(t){return this.rotateOnAxis(ES,t)}translateOnAxis(t,i){return xS.copy(t).applyQuaternion(this.quaternion),this.position.add(xS.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(MS,t)}translateY(t){return this.translateOnAxis(bS,t)}translateZ(t){return this.translateOnAxis(ES,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(gr.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?cf.copy(t):cf.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Jc.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?gr.lookAt(Jc,cf,this.up):gr.lookAt(cf,Jc,this.up),this.quaternion.setFromRotationMatrix(gr),s&&(gr.extractRotation(s.matrixWorld),sa.setFromRotationMatrix(gr),this.quaternion.premultiply(sa.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Re("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(SS),oa.child=t,this.dispatchEvent(oa),oa.child=null):Re("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(NP),gy.child=t,this.dispatchEvent(gy),gy.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),gr.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),gr.multiply(t.parent.matrixWorld)),t.applyMatrix4(gr),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(SS),oa.child=t,this.dispatchEvent(oa),oa.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Jc,t,IP),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Jc,RP,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>et(ae({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>ae({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let f=l[u];o(t.shapes,f)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),f=a(t.shapes),h=a(t.skeletons),g=a(t.animations),_=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),f.length>0&&(r.shapes=f),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),_.length>0&&(r.nodes=_)}return r.object=s,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),t.pivot!==null&&(this.pivot=t.pivot.clone()),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new P(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Bi=class extends qi{constructor(){super(),this.isGroup=!0,this.type="Group"}},PP={type:"move"},Ea=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new Bi,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new Bi,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new P,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new P),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new Bi,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new P,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new P),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let _ of e.hand.values()){let m=t.getJointPose(_,i),p=this._getHandJoint(l,_);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,g=.005;l.inputState.pinching&&f>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&f<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(PP)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new Bi;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},_w={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},qr={h:0,s:0,l:0},lf={h:0,s:0,l:0};function vy(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var We=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Hn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,nt.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=nt.workingColorSpace){return this.r=e,this.g=t,this.b=i,nt.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=nt.workingColorSpace){if(e=l_(e,1),t=Qe(t,0,1),i=Qe(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=vy(o,s,e+1/3),this.g=vy(o,s,e),this.b=vy(o,s,e-1/3)}return nt.colorSpaceToWorking(this,r),this}setStyle(e,t=Hn){function i(s){s!==void 0&&parseFloat(s)<1&&Ae("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:Ae("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);Ae("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Hn){let i=_w[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Ae("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Er(e.r),this.g=Er(e.g),this.b=Er(e.b),this}copyLinearToSRGB(e){return this.r=_a(e.r),this.g=_a(e.g),this.b=_a(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Hn){return nt.workingToColorSpace(mn.copy(this),e),Math.round(Qe(mn.r*255,0,255))*65536+Math.round(Qe(mn.g*255,0,255))*256+Math.round(Qe(mn.b*255,0,255))}getHexString(e=Hn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=nt.workingColorSpace){nt.workingToColorSpace(mn.copy(this),t);let i=mn.r,r=mn.g,s=mn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let d=o-a;switch(l=u<=.5?d/(o+a):d/(2-o-a),o){case i:c=(r-s)/d+(r<s?6:0);break;case r:c=(s-i)/d+2;break;case s:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=nt.workingColorSpace){return nt.workingToColorSpace(mn.copy(this),t),e.r=mn.r,e.g=mn.g,e.b=mn.b,e}getStyle(e=Hn){nt.workingToColorSpace(mn.copy(this),e);let t=mn.r,i=mn.g,r=mn.b;return e!==Hn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(qr),this.setHSL(qr.h+e,qr.s+t,qr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(qr),e.getHSL(lf);let i=al(qr.h,lf.h,t),r=al(qr.s,lf.s,t),s=al(qr.l,lf.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},mn=new We;We.NAMES=_w;var gl=class extends qi{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new Qr,this.environmentIntensity=1,this.environmentRotation=new Qr,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},mi=new P,vr=new P,yy=new P,yr=new P,aa=new P,ca=new P,wS=new P,_y=new P,xy=new P,My=new P,by=new Nt,Ey=new Nt,Sy=new Nt,Mr=class n{constructor(e=new P,t=new P,i=new P){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),mi.subVectors(e,t),r.cross(mi);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){mi.subVectors(r,t),vr.subVectors(i,t),yy.subVectors(e,t);let o=mi.dot(mi),a=mi.dot(vr),c=mi.dot(yy),l=vr.dot(vr),u=vr.dot(yy),d=o*l-a*a;if(d===0)return s.set(0,0,0),null;let f=1/d,h=(l*c-a*u)*f,g=(o*u-a*c)*f;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,yr)===null?!1:yr.x>=0&&yr.y>=0&&yr.x+yr.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,yr)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,yr.x),c.addScaledVector(o,yr.y),c.addScaledVector(a,yr.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return by.setScalar(0),Ey.setScalar(0),Sy.setScalar(0),by.fromBufferAttribute(e,t),Ey.fromBufferAttribute(e,i),Sy.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(by,s.x),o.addScaledVector(Ey,s.y),o.addScaledVector(Sy,s.z),o}static isFrontFacing(e,t,i,r){return mi.subVectors(i,t),vr.subVectors(e,t),mi.cross(vr).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return mi.subVectors(this.c,this.b),vr.subVectors(this.a,this.b),mi.cross(vr).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;aa.subVectors(r,i),ca.subVectors(s,i),_y.subVectors(e,i);let c=aa.dot(_y),l=ca.dot(_y);if(c<=0&&l<=0)return t.copy(i);xy.subVectors(e,r);let u=aa.dot(xy),d=ca.dot(xy);if(u>=0&&d<=u)return t.copy(r);let f=c*d-u*l;if(f<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(aa,o);My.subVectors(e,s);let h=aa.dot(My),g=ca.dot(My);if(g>=0&&h<=g)return t.copy(s);let _=h*l-c*g;if(_<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(ca,a);let m=u*g-h*d;if(m<=0&&d-u>=0&&h-g>=0)return wS.subVectors(s,r),a=(d-u)/(d-u+(h-g)),t.copy(r).addScaledVector(wS,a);let p=1/(m+_+f);return o=_*p,a=f*p,t.copy(i).addScaledVector(aa,o).addScaledVector(ca,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},es=class{constructor(e=new P(1/0,1/0,1/0),t=new P(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(gi.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(gi.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=gi.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,gi):gi.fromBufferAttribute(s,o),gi.applyMatrix4(e.matrixWorld),this.expandByPoint(gi);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),uf.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),uf.copy(i.boundingBox)),uf.applyMatrix4(e.matrixWorld),this.union(uf)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,gi),gi.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(Qc),df.subVectors(this.max,Qc),la.subVectors(e.a,Qc),ua.subVectors(e.b,Qc),da.subVectors(e.c,Qc),Xr.subVectors(ua,la),Yr.subVectors(da,ua),Ws.subVectors(la,da);let t=[0,-Xr.z,Xr.y,0,-Yr.z,Yr.y,0,-Ws.z,Ws.y,Xr.z,0,-Xr.x,Yr.z,0,-Yr.x,Ws.z,0,-Ws.x,-Xr.y,Xr.x,0,-Yr.y,Yr.x,0,-Ws.y,Ws.x,0];return!wy(t,la,ua,da,df)||(t=[1,0,0,0,1,0,0,0,1],!wy(t,la,ua,da,df))?!1:(ff.crossVectors(Xr,Yr),t=[ff.x,ff.y,ff.z],wy(t,la,ua,da,df))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,gi).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(gi).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(_r[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),_r[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),_r[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),_r[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),_r[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),_r[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),_r[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),_r[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(_r),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},_r=[new P,new P,new P,new P,new P,new P,new P,new P],gi=new P,uf=new es,la=new P,ua=new P,da=new P,Xr=new P,Yr=new P,Ws=new P,Qc=new P,df=new P,ff=new P,$s=new P;function wy(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){$s.fromArray(n,s);let a=r.x*Math.abs($s.x)+r.y*Math.abs($s.y)+r.z*Math.abs($s.z),c=e.dot($s),l=t.dot($s),u=i.dot($s);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var Bt=new P,hf=new Ne,OP=0,An=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:OP++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=Wf,this.updateRanges=[],this.gpuType=Mi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)hf.fromBufferAttribute(this,t),hf.applyMatrix3(e),this.setXY(t,hf.x,hf.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Bt.fromBufferAttribute(this,t),Bt.applyMatrix3(e),this.setXYZ(t,Bt.x,Bt.y,Bt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Bt.fromBufferAttribute(this,t),Bt.applyMatrix4(e),this.setXYZ(t,Bt.x,Bt.y,Bt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Bt.fromBufferAttribute(this,t),Bt.applyNormalMatrix(e),this.setXYZ(t,Bt.x,Bt.y,Bt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Bt.fromBufferAttribute(this,t),Bt.transformDirection(e),this.setXYZ(t,Bt.x,Bt.y,Bt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=vi(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=mt(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=vi(t,this.array)),t}setX(e,t){return this.normalized&&(t=mt(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=vi(t,this.array)),t}setY(e,t){return this.normalized&&(t=mt(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=vi(t,this.array)),t}setZ(e,t){return this.normalized&&(t=mt(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=vi(t,this.array)),t}setW(e,t){return this.normalized&&(t=mt(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=mt(t,this.array),i=mt(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=mt(t,this.array),i=mt(i,this.array),r=mt(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=mt(t,this.array),i=mt(i,this.array),r=mt(r,this.array),s=mt(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==Wf&&(e.usage=this.usage),e}};var vl=class extends An{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var yl=class extends An{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var jt=class extends An{constructor(e,t,i){super(new Float32Array(e),t,i)}},LP=new es,el=new P,Cy=new P,Qs=class{constructor(e=new P,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):LP.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;el.subVectors(e,this.center);let t=el.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(el,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Cy.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(el.copy(e.center).add(Cy)),this.expandByPoint(el.copy(e.center).sub(Cy))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},FP=0,ti=new Rt,Dy=new qi,fa=new P,Vn=new es,tl=new es,Qt=new P,vn=class n extends Hi{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:FP++}),this.uuid=br(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(oP(e)?yl:vl)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new ze().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return ti.makeRotationFromQuaternion(e),this.applyMatrix4(ti),this}rotateX(e){return ti.makeRotationX(e),this.applyMatrix4(ti),this}rotateY(e){return ti.makeRotationY(e),this.applyMatrix4(ti),this}rotateZ(e){return ti.makeRotationZ(e),this.applyMatrix4(ti),this}translate(e,t,i){return ti.makeTranslation(e,t,i),this.applyMatrix4(ti),this}scale(e,t,i){return ti.makeScale(e,t,i),this.applyMatrix4(ti),this}lookAt(e){return Dy.lookAt(e),Dy.updateMatrix(),this.applyMatrix4(Dy.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(fa).negate(),this.translate(fa.x,fa.y,fa.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new jt(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&Ae("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new es);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Re("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new P(-1/0,-1/0,-1/0),new P(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];Vn.setFromBufferAttribute(s),this.morphTargetsRelative?(Qt.addVectors(this.boundingBox.min,Vn.min),this.boundingBox.expandByPoint(Qt),Qt.addVectors(this.boundingBox.max,Vn.max),this.boundingBox.expandByPoint(Qt)):(this.boundingBox.expandByPoint(Vn.min),this.boundingBox.expandByPoint(Vn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Re('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Qs);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Re("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new P,1/0);return}if(e){let i=this.boundingSphere.center;if(Vn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];tl.setFromBufferAttribute(a),this.morphTargetsRelative?(Qt.addVectors(Vn.min,tl.min),Vn.expandByPoint(Qt),Qt.addVectors(Vn.max,tl.max),Vn.expandByPoint(Qt)):(Vn.expandByPoint(tl.min),Vn.expandByPoint(tl.max))}Vn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Qt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Qt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)Qt.fromBufferAttribute(a,l),c&&(fa.fromBufferAttribute(e,l),Qt.add(fa)),r=Math.max(r,i.distanceToSquared(Qt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Re('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Re("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new An(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let y=0;y<i.count;y++)a[y]=new P,c[y]=new P;let l=new P,u=new P,d=new P,f=new Ne,h=new Ne,g=new Ne,_=new P,m=new P;function p(y,E,q){l.fromBufferAttribute(i,y),u.fromBufferAttribute(i,E),d.fromBufferAttribute(i,q),f.fromBufferAttribute(s,y),h.fromBufferAttribute(s,E),g.fromBufferAttribute(s,q),u.sub(l),d.sub(l),h.sub(f),g.sub(f);let C=1/(h.x*g.y-g.x*h.y);isFinite(C)&&(_.copy(u).multiplyScalar(g.y).addScaledVector(d,-h.y).multiplyScalar(C),m.copy(d).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(C),a[y].add(_),a[E].add(_),a[q].add(_),c[y].add(m),c[E].add(m),c[q].add(m))}let M=this.groups;M.length===0&&(M=[{start:0,count:e.count}]);for(let y=0,E=M.length;y<E;++y){let q=M[y],C=q.start,U=q.count;for(let V=C,j=C+U;V<j;V+=3)p(e.getX(V+0),e.getX(V+1),e.getX(V+2))}let w=new P,S=new P,T=new P,D=new P;function I(y){T.fromBufferAttribute(r,y),D.copy(T);let E=a[y];w.copy(E),w.sub(T.multiplyScalar(T.dot(E))).normalize(),S.crossVectors(D,E);let C=S.dot(c[y])<0?-1:1;o.setXYZW(y,w.x,w.y,w.z,C)}for(let y=0,E=M.length;y<E;++y){let q=M[y],C=q.start,U=q.count;for(let V=C,j=C+U;V<j;V+=3)I(e.getX(V+0)),I(e.getX(V+1)),I(e.getX(V+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new An(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new P,s=new P,o=new P,a=new P,c=new P,l=new P,u=new P,d=new P;if(e)for(let f=0,h=e.count;f<h;f+=3){let g=e.getX(f+0),_=e.getX(f+1),m=e.getX(f+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,_),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,_),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(_,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),s.fromBufferAttribute(t,f+1),o.fromBufferAttribute(t,f+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Qt.fromBufferAttribute(e,t),Qt.normalize(),e.setXYZ(t,Qt.x,Qt.y,Qt.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,f=new l.constructor(c.length*u),h=0,g=0;for(let _=0,m=c.length;_<m;_++){a.isInterleavedBufferAttribute?h=c[_]*a.data.stride+a.offset:h=c[_]*u;for(let p=0;p<u;p++)f[g++]=l[h++]}return new An(f,u,d)}if(this.index===null)return Ae("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,d=l.length;u<d;u++){let f=l[u],h=e(f,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,f=l.length;d<f;d++){let h=l[d];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],d=s[l];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let d=o[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},Yf=class{constructor(e,t){this.isInterleavedBuffer=!0,this.array=e,this.stride=t,this.count=e!==void 0?e.length/t:0,this.usage=Wf,this.updateRanges=[],this.version=0,this.uuid=br()}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.array=new e.array.constructor(e.array),this.count=e.count,this.stride=e.stride,this.usage=e.usage,this}copyAt(e,t,i){e*=this.stride,i*=t.stride;for(let r=0,s=this.stride;r<s;r++)this.array[e+r]=t.array[i+r];return this}set(e,t=0){return this.array.set(e,t),this}clone(e){e.arrayBuffers===void 0&&(e.arrayBuffers={}),this.array.buffer._uuid===void 0&&(this.array.buffer._uuid=br()),e.arrayBuffers[this.array.buffer._uuid]===void 0&&(e.arrayBuffers[this.array.buffer._uuid]=this.array.slice(0).buffer);let t=new this.array.constructor(e.arrayBuffers[this.array.buffer._uuid]),i=new this.constructor(t,this.stride);return i.setUsage(this.usage),i}onUpload(e){return this.onUploadCallback=e,this}toJSON(e){return e.arrayBuffers===void 0&&(e.arrayBuffers={}),this.array.buffer._uuid===void 0&&(this.array.buffer._uuid=br()),e.arrayBuffers[this.array.buffer._uuid]===void 0&&(e.arrayBuffers[this.array.buffer._uuid]=Array.from(new Uint32Array(this.array.buffer))),{uuid:this.uuid,buffer:this.array.buffer._uuid,type:this.array.constructor.name,stride:this.stride}}},Sn=new P,_l=class n{constructor(e,t,i,r=!1){this.isInterleavedBufferAttribute=!0,this.name="",this.data=e,this.itemSize=t,this.offset=i,this.normalized=r}get count(){return this.data.count}get array(){return this.data.array}set needsUpdate(e){this.data.needsUpdate=e}applyMatrix4(e){for(let t=0,i=this.data.count;t<i;t++)Sn.fromBufferAttribute(this,t),Sn.applyMatrix4(e),this.setXYZ(t,Sn.x,Sn.y,Sn.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Sn.fromBufferAttribute(this,t),Sn.applyNormalMatrix(e),this.setXYZ(t,Sn.x,Sn.y,Sn.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Sn.fromBufferAttribute(this,t),Sn.transformDirection(e),this.setXYZ(t,Sn.x,Sn.y,Sn.z);return this}getComponent(e,t){let i=this.array[e*this.data.stride+this.offset+t];return this.normalized&&(i=vi(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=mt(i,this.array)),this.data.array[e*this.data.stride+this.offset+t]=i,this}setX(e,t){return this.normalized&&(t=mt(t,this.array)),this.data.array[e*this.data.stride+this.offset]=t,this}setY(e,t){return this.normalized&&(t=mt(t,this.array)),this.data.array[e*this.data.stride+this.offset+1]=t,this}setZ(e,t){return this.normalized&&(t=mt(t,this.array)),this.data.array[e*this.data.stride+this.offset+2]=t,this}setW(e,t){return this.normalized&&(t=mt(t,this.array)),this.data.array[e*this.data.stride+this.offset+3]=t,this}getX(e){let t=this.data.array[e*this.data.stride+this.offset];return this.normalized&&(t=vi(t,this.array)),t}getY(e){let t=this.data.array[e*this.data.stride+this.offset+1];return this.normalized&&(t=vi(t,this.array)),t}getZ(e){let t=this.data.array[e*this.data.stride+this.offset+2];return this.normalized&&(t=vi(t,this.array)),t}getW(e){let t=this.data.array[e*this.data.stride+this.offset+3];return this.normalized&&(t=vi(t,this.array)),t}setXY(e,t,i){return e=e*this.data.stride+this.offset,this.normalized&&(t=mt(t,this.array),i=mt(i,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this}setXYZ(e,t,i,r){return e=e*this.data.stride+this.offset,this.normalized&&(t=mt(t,this.array),i=mt(i,this.array),r=mt(r,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this.data.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e=e*this.data.stride+this.offset,this.normalized&&(t=mt(t,this.array),i=mt(i,this.array),r=mt(r,this.array),s=mt(s,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this.data.array[e+2]=r,this.data.array[e+3]=s,this}clone(e){if(e===void 0){fl("InterleavedBufferAttribute.clone(): Cloning an interleaved buffer attribute will de-interleave buffer data.");let t=[];for(let i=0;i<this.count;i++){let r=i*this.data.stride+this.offset;for(let s=0;s<this.itemSize;s++)t.push(this.data.array[r+s])}return new An(new this.array.constructor(t),this.itemSize,this.normalized)}else return e.interleavedBuffers===void 0&&(e.interleavedBuffers={}),e.interleavedBuffers[this.data.uuid]===void 0&&(e.interleavedBuffers[this.data.uuid]=this.data.clone(e)),new n(e.interleavedBuffers[this.data.uuid],this.itemSize,this.offset,this.normalized)}toJSON(e){if(e===void 0){fl("InterleavedBufferAttribute.toJSON(): Serializing an interleaved buffer attribute will de-interleave buffer data.");let t=[];for(let i=0;i<this.count;i++){let r=i*this.data.stride+this.offset;for(let s=0;s<this.itemSize;s++)t.push(this.data.array[r+s])}return{itemSize:this.itemSize,type:this.array.constructor.name,array:t,normalized:this.normalized}}else return e.interleavedBuffers===void 0&&(e.interleavedBuffers={}),e.interleavedBuffers[this.data.uuid]===void 0&&(e.interleavedBuffers[this.data.uuid]=this.data.toJSON(e)),{isInterleavedBufferAttribute:!0,itemSize:this.itemSize,data:this.data.uuid,offset:this.offset,normalized:this.normalized}}},kP=0,zi=class extends Hi{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:kP++}),this.uuid=br(),this.name="",this.type="Material",this.blending=Zs,this.side=Sr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Pf,this.blendDst=Of,this.blendEquation=Jr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new We(0,0,0),this.blendAlpha=0,this.depthFunc=Ks,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Vy,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Ys,this.stencilZFail=Ys,this.stencilZPass=Ys,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Ae(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Ae(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==Zs&&(i.blending=this.blending),this.side!==Sr&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Pf&&(i.blendSrc=this.blendSrc),this.blendDst!==Of&&(i.blendDst=this.blendDst),this.blendEquation!==Jr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==Ks&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Vy&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Ys&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Ys&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Ys&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}},eo=class extends zi{constructor(e){super(),this.isSpriteMaterial=!0,this.type="SpriteMaterial",this.color=new We(16777215),this.map=null,this.alphaMap=null,this.rotation=0,this.sizeAttenuation=!0,this.transparent=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.rotation=e.rotation,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},ha,nl=new P,pa=new P,ma=new P,ga=new Ne,il=new Ne,xw=new Rt,pf=new P,rl=new P,mf=new P,CS=new Ne,Ty=new Ne,DS=new Ne,Sa=class extends qi{constructor(e=new eo){if(super(),this.isSprite=!0,this.type="Sprite",ha===void 0){ha=new vn;let t=new Float32Array([-.5,-.5,0,0,0,.5,-.5,0,1,0,.5,.5,0,1,1,-.5,.5,0,0,1]),i=new Yf(t,5);ha.setIndex([0,1,2,0,2,3]),ha.setAttribute("position",new _l(i,3,0,!1)),ha.setAttribute("uv",new _l(i,2,3,!1))}this.geometry=ha,this.material=e,this.center=new Ne(.5,.5),this.count=1}raycast(e,t){e.camera===null&&Re('Sprite: "Raycaster.camera" needs to be set in order to raycast against sprites.'),pa.setFromMatrixScale(this.matrixWorld),xw.copy(e.camera.matrixWorld),this.modelViewMatrix.multiplyMatrices(e.camera.matrixWorldInverse,this.matrixWorld),ma.setFromMatrixPosition(this.modelViewMatrix),e.camera.isPerspectiveCamera&&this.material.sizeAttenuation===!1&&pa.multiplyScalar(-ma.z);let i=this.material.rotation,r,s;i!==0&&(s=Math.cos(i),r=Math.sin(i));let o=this.center;gf(pf.set(-.5,-.5,0),ma,o,pa,r,s),gf(rl.set(.5,-.5,0),ma,o,pa,r,s),gf(mf.set(.5,.5,0),ma,o,pa,r,s),CS.set(0,0),Ty.set(1,0),DS.set(1,1);let a=e.ray.intersectTriangle(pf,rl,mf,!1,nl);if(a===null&&(gf(rl.set(-.5,.5,0),ma,o,pa,r,s),Ty.set(0,1),a=e.ray.intersectTriangle(pf,mf,rl,!1,nl),a===null))return;let c=e.ray.origin.distanceTo(nl);c<e.near||c>e.far||t.push({distance:c,point:nl.clone(),uv:Mr.getInterpolation(nl,pf,rl,mf,CS,Ty,DS,new Ne),face:null,object:this})}copy(e,t){return super.copy(e,t),e.center!==void 0&&this.center.copy(e.center),this.material=e.material,this}};function gf(n,e,t,i,r,s){ga.subVectors(n,t).addScalar(.5).multiply(i),r!==void 0?(il.x=s*ga.x-r*ga.y,il.y=r*ga.x+s*ga.y):il.copy(ga),n.copy(e),n.x+=il.x,n.y+=il.y,n.applyMatrix4(xw)}var xr=new P,Ay=new P,vf=new P,Zr=new P,Iy=new P,yf=new P,Ry=new P,to=class{constructor(e=new P,t=new P(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,xr)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=xr.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(xr.copy(this.origin).addScaledVector(this.direction,t),xr.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Ay.copy(e).add(t).multiplyScalar(.5),vf.copy(t).sub(e).normalize(),Zr.copy(this.origin).sub(Ay);let s=e.distanceTo(t)*.5,o=-this.direction.dot(vf),a=Zr.dot(this.direction),c=-Zr.dot(vf),l=Zr.lengthSq(),u=Math.abs(1-o*o),d,f,h,g;if(u>0)if(d=o*c-a,f=o*a-c,g=s*u,d>=0)if(f>=-g)if(f<=g){let _=1/u;d*=_,f*=_,h=d*(d+o*f+2*a)+f*(o*d+f+2*c)+l}else f=s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f=-s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f<=-g?(d=Math.max(0,-(-o*s+a)),f=d>0?-s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l):f<=g?(d=0,f=Math.min(Math.max(-s,-c),s),h=f*(f+2*c)+l):(d=Math.max(0,-(o*s+a)),f=d>0?s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l);else f=o>0?-s:s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Ay).addScaledVector(vf,f),h}intersectSphere(e,t){xr.subVectors(e.center,this.origin);let i=xr.dot(this.direction),r=xr.dot(xr)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return l>=0?(i=(e.min.x-f.x)*l,r=(e.max.x-f.x)*l):(i=(e.max.x-f.x)*l,r=(e.min.x-f.x)*l),u>=0?(s=(e.min.y-f.y)*u,o=(e.max.y-f.y)*u):(s=(e.max.y-f.y)*u,o=(e.min.y-f.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-f.z)*d,c=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,c=(e.min.z-f.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,xr)!==null}intersectTriangle(e,t,i,r,s){Iy.subVectors(t,e),yf.subVectors(i,e),Ry.crossVectors(Iy,yf);let o=this.direction.dot(Ry),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;Zr.subVectors(this.origin,e);let c=a*this.direction.dot(yf.crossVectors(Zr,yf));if(c<0)return null;let l=a*this.direction.dot(Iy.cross(Zr));if(l<0||c+l>o)return null;let u=-a*Zr.dot(Ry);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},no=class extends zi{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new We(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Qr,this.combine=$y,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},TS=new Rt,qs=new to,_f=new Qs,AS=new P,xf=new P,Mf=new P,bf=new P,Ny=new P,Ef=new P,IS=new P,Sf=new P,wn=class extends qi{constructor(e=new vn,t=new no){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){Ef.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],d=s[c];u!==0&&(Ny.fromBufferAttribute(d,e),o?Ef.addScaledVector(Ny,u):Ef.addScaledVector(Ny.sub(t),u))}t.add(Ef)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),_f.copy(i.boundingSphere),_f.applyMatrix4(s),qs.copy(e.ray).recast(e.near),!(_f.containsPoint(qs.origin)===!1&&(qs.intersectSphere(_f,AS)===null||qs.origin.distanceToSquared(AS)>(e.far-e.near)**2))&&(TS.copy(s).invert(),qs.copy(e.ray).applyMatrix4(TS),!(i.boundingBox!==null&&qs.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,qs)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,f=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,_=f.length;g<_;g++){let m=f[g],p=o[m.materialIndex],M=Math.max(m.start,h.start),w=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let S=M,T=w;S<T;S+=3){let D=a.getX(S),I=a.getX(S+1),y=a.getX(S+2);r=wf(this,p,e,i,l,u,d,D,I,y),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),_=Math.min(a.count,h.start+h.count);for(let m=g,p=_;m<p;m+=3){let M=a.getX(m),w=a.getX(m+1),S=a.getX(m+2);r=wf(this,o,e,i,l,u,d,M,w,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,_=f.length;g<_;g++){let m=f[g],p=o[m.materialIndex],M=Math.max(m.start,h.start),w=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let S=M,T=w;S<T;S+=3){let D=S,I=S+1,y=S+2;r=wf(this,p,e,i,l,u,d,D,I,y),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),_=Math.min(c.count,h.start+h.count);for(let m=g,p=_;m<p;m+=3){let M=m,w=m+1,S=m+2;r=wf(this,o,e,i,l,u,d,M,w,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function UP(n,e,t,i,r,s,o,a){let c;if(e.side===Cn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===Sr,a),c===null)return null;Sf.copy(a),Sf.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(Sf);return l<t.near||l>t.far?null:{distance:l,point:Sf.clone(),object:n}}function wf(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,xf),n.getVertexPosition(c,Mf),n.getVertexPosition(l,bf);let u=UP(n,e,t,i,xf,Mf,bf,IS);if(u){let d=new P;Mr.getBarycoord(IS,xf,Mf,bf,d),r&&(u.uv=Mr.getInterpolatedAttribute(r,a,c,l,d,new Ne)),s&&(u.uv1=Mr.getInterpolatedAttribute(s,a,c,l,d,new Ne)),o&&(u.normal=Mr.getInterpolatedAttribute(o,a,c,l,d,new P),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:c,c:l,normal:new P,materialIndex:0};Mr.getNormal(xf,Mf,bf,f.normal),u.face=f,u.barycoord=d}return u}var Zf=class extends $i{constructor(e=null,t=1,i=1,r,s,o,a,c,l=en,u=en,d,f){super(null,o,a,c,l,u,r,s,d,f),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Py=new P,BP=new P,VP=new ze,ni=class{constructor(e=new P(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Py.subVectors(i,t).cross(BP.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(Py),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||VP.getNormalMatrix(e),r=this.coplanarPoint(Py).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},Xs=new Qs,HP=new Ne(.5,.5),Cf=new P,xl=class{constructor(e=new ni,t=new ni,i=new ni,r=new ni,s=new ni,o=new ni){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=yi,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],d=s[5],f=s[6],h=s[7],g=s[8],_=s[9],m=s[10],p=s[11],M=s[12],w=s[13],S=s[14],T=s[15];if(r[0].setComponents(l-o,h-u,p-g,T-M).normalize(),r[1].setComponents(l+o,h+u,p+g,T+M).normalize(),r[2].setComponents(l+a,h+d,p+_,T+w).normalize(),r[3].setComponents(l-a,h-d,p-_,T-w).normalize(),i)r[4].setComponents(c,f,m,S).normalize(),r[5].setComponents(l-c,h-f,p-m,T-S).normalize();else if(r[4].setComponents(l-c,h-f,p-m,T-S).normalize(),t===yi)r[5].setComponents(l+c,h+f,p+m,T+S).normalize();else if(t===ul)r[5].setComponents(c,f,m,S).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Xs.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Xs.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Xs)}intersectsSprite(e){Xs.center.set(0,0,0);let t=HP.distanceTo(e.center);return Xs.radius=.7071067811865476+t,Xs.applyMatrix4(e.matrixWorld),this.intersectsSphere(Xs)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Cf.x=r.normal.x>0?e.max.x:e.min.x,Cf.y=r.normal.y>0?e.max.y:e.min.y,Cf.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Cf)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var io=class extends zi{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new We(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},Kf=new P,Jf=new P,RS=new Rt,sl=new to,Df=new Qs,Oy=new P,NS=new P,wa=class extends qi{constructor(e=new vn,t=new io){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,s=t.count;r<s;r++)Kf.fromBufferAttribute(t,r-1),Jf.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=Kf.distanceTo(Jf);e.setAttribute("lineDistance",new jt(i,1))}else Ae("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Line.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),Df.copy(i.boundingSphere),Df.applyMatrix4(r),Df.radius+=s,e.ray.intersectsSphere(Df)===!1)return;RS.copy(r).invert(),sl.copy(e.ray).applyMatrix4(RS);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,f=i.attributes.position;if(u!==null){let h=Math.max(0,o.start),g=Math.min(u.count,o.start+o.count);for(let _=h,m=g-1;_<m;_+=l){let p=u.getX(_),M=u.getX(_+1),w=Tf(this,e,sl,c,p,M,_);w&&t.push(w)}if(this.isLineLoop){let _=u.getX(g-1),m=u.getX(h),p=Tf(this,e,sl,c,_,m,g-1);p&&t.push(p)}}else{let h=Math.max(0,o.start),g=Math.min(f.count,o.start+o.count);for(let _=h,m=g-1;_<m;_+=l){let p=Tf(this,e,sl,c,_,_+1,_);p&&t.push(p)}if(this.isLineLoop){let _=Tf(this,e,sl,c,g-1,h,g-1);_&&t.push(_)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function Tf(n,e,t,i,r,s,o){let a=n.geometry.attributes.position;if(Kf.fromBufferAttribute(a,r),Jf.fromBufferAttribute(a,s),t.distanceSqToSegment(Kf,Jf,Oy,NS)>i)return;Oy.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(Oy);if(!(l<e.near||l>e.far))return{distance:l,point:NS.clone().applyMatrix4(n.matrixWorld),index:o,face:null,faceIndex:null,barycoord:null,object:n}}var PS=new P,OS=new P,Qf=class extends wa{constructor(e,t){super(e,t),this.isLineSegments=!0,this.type="LineSegments"}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[];for(let r=0,s=t.count;r<s;r+=2)PS.fromBufferAttribute(t,r),OS.fromBufferAttribute(t,r+1),i[r]=r===0?0:i[r-1],i[r+1]=i[r]+PS.distanceTo(OS);e.setAttribute("lineDistance",new jt(i,1))}else Ae("LineSegments.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}};var Ml=class extends $i{constructor(e=[],t=as,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},Ca=class extends $i{constructor(e,t,i,r,s,o,a,c,l){super(e,t,i,r,s,o,a,c,l),this.isCanvasTexture=!0,this.needsUpdate=!0}},ts=class extends $i{constructor(e,t,i=xi,r,s,o,a=en,c=en,l,u=Vi,d=1){if(u!==Vi&&u!==ls)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let f={width:e,height:t,depth:d};super(f,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new ba(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},eh=class extends ts{constructor(e,t=xi,i=as,r,s,o=en,a=en,c,l=Vi){let u={width:e,height:e,depth:1},d=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=d,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},bl=class extends $i{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},Da=class n extends vn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],d=[],f=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new jt(l,3)),this.setAttribute("normal",new jt(u,3)),this.setAttribute("uv",new jt(d,2));function g(_,m,p,M,w,S,T,D,I,y,E){let q=S/I,C=T/y,U=S/2,V=T/2,j=D/2,B=I+1,H=y+1,L=0,Q=0,Z=new P;for(let de=0;de<H;de++){let ge=de*C-V;for(let he=0;he<B;he++){let $e=he*q-U;Z[_]=$e*M,Z[m]=ge*w,Z[p]=j,l.push(Z.x,Z.y,Z.z),Z[_]=0,Z[m]=0,Z[p]=D>0?1:-1,u.push(Z.x,Z.y,Z.z),d.push(he/I),d.push(1-de/y),L+=1}}for(let de=0;de<y;de++)for(let ge=0;ge<I;ge++){let he=f+ge+B*de,$e=f+ge+B*(de+1),Ct=f+(ge+1)+B*(de+1),wt=f+(ge+1)+B*de;c.push(he,$e,wt),c.push($e,Ct,wt),Q+=6}a.addGroup(h,Q,E),h+=Q,f+=L}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var El=class n extends vn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,f=t/c,h=[],g=[],_=[],m=[];for(let p=0;p<u;p++){let M=p*f-o;for(let w=0;w<l;w++){let S=w*d-s;g.push(S,-M,0),_.push(0,0,1),m.push(w/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let M=0;M<a;M++){let w=M+l*p,S=M+l*(p+1),T=M+1+l*(p+1),D=M+1+l*p;h.push(w,S,D),h.push(S,T,D)}this.setIndex(h),this.setAttribute("position",new jt(g,3)),this.setAttribute("normal",new jt(_,3)),this.setAttribute("uv",new jt(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};var Ta=class n extends vn{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let c=Math.min(o+a,Math.PI),l=0,u=[],d=new P,f=new P,h=[],g=[],_=[],m=[];for(let p=0;p<=i;p++){let M=[],w=p/i,S=0;p===0&&o===0?S=.5/t:p===i&&c===Math.PI&&(S=-.5/t);for(let T=0;T<=t;T++){let D=T/t;d.x=-e*Math.cos(r+D*s)*Math.sin(o+w*a),d.y=e*Math.cos(o+w*a),d.z=e*Math.sin(r+D*s)*Math.sin(o+w*a),g.push(d.x,d.y,d.z),f.copy(d).normalize(),_.push(f.x,f.y,f.z),m.push(D+S,1-w),M.push(l++)}u.push(M)}for(let p=0;p<i;p++)for(let M=0;M<t;M++){let w=u[p][M+1],S=u[p][M],T=u[p+1][M],D=u[p+1][M+1];(p!==0||o>0)&&h.push(w,S,D),(p!==i-1||c<Math.PI)&&h.push(S,T,D)}this.setIndex(h),this.setAttribute("position",new jt(g,3)),this.setAttribute("normal",new jt(_,3)),this.setAttribute("uv",new jt(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}};function oo(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(Ae("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function yn(n){let e={};for(let t=0;t<n.length;t++){let i=oo(n[t]);for(let r in i)e[r]=i[r]}return e}function zP(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function d_(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:nt.workingColorSpace}var Mw={clone:oo,merge:yn},GP=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,jP=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,jn=class extends zi{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=GP,this.fragmentShader=jP,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=oo(e.uniforms),this.uniformsGroups=zP(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},th=class extends jn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}},Sl=class extends zi{constructor(e){super(),this.isMeshStandardMaterial=!0,this.type="MeshStandardMaterial",this.defines={STANDARD:""},this.color=new We(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new We(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=a_,this.normalScale=new Ne(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Qr,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};var nh=class extends zi{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=aw,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},ih=class extends zi{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function Af(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var ns=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},rh=class extends ns{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:ky,endingEnd:ky}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Uy:s=e,a=2*t-i;break;case By:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case Uy:o=e,c=2*i-t;break;case By:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),_=g*g,m=_*g,p=-f*m+2*f*_-f*g,M=(1+f)*m+(-1.5-2*f)*_+(-.5+f)*g+1,w=(-1-h)*m+(1.5+h)*_+.5*g,S=h*m-h*_;for(let T=0;T!==a;++T)s[T]=p*o[u+T]+M*o[l+T]+w*o[c+T]+S*o[d+T];return s}},sh=class extends ns{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)s[f]=o[l+f]*d+o[c+f]*u;return s}},oh=class extends ns{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},ah=class extends ns{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,d=u.inTangents,f=u.outTangents;if(!d||!f){let _=(i-t)/(r-t),m=1-_;for(let p=0;p!==a;++p)s[p]=o[l+p]*m+o[c+p]*_;return s}let h=a*2,g=e-1;for(let _=0;_!==a;++_){let m=o[l+_],p=o[c+_],M=g*h+_*2,w=f[M],S=f[M+1],T=e*h+_*2,D=d[T],I=d[T+1],y=(i-t)/(r-t),E,q,C,U,V;for(let j=0;j<8;j++){E=y*y,q=E*y,C=1-y,U=C*C,V=U*C;let H=V*t+3*U*y*w+3*C*E*D+q*r-i;if(Math.abs(H)<1e-10)break;let L=3*U*(w-t)+6*C*y*(D-w)+3*E*(r-D);if(Math.abs(L)<1e-10)break;y=y-H/L,y=Math.max(0,Math.min(1,y))}s[_]=V*m+3*U*y*S+3*C*E*I+q*p}return s}},Wn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Af(t,this.TimeBufferType),this.values=Af(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Af(e.times,Array),values:Af(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new oh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new sh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new rh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new ah(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case cl:t=this.InterpolantFactoryMethodDiscrete;break;case jf:t=this.InterpolantFactoryMethodLinear;break;case Nf:t=this.InterpolantFactoryMethodSmooth;break;case Fy:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Ae("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return cl;case this.InterpolantFactoryMethodLinear:return jf;case this.InterpolantFactoryMethodSmooth:return Nf;case this.InterpolantFactoryMethodBezier:return Fy}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Re("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Re("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Re("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Re("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&aP(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Re("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===Nf,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,f=d-i,h=d+i;for(let g=0;g!==i;++g){let _=t[d+g];if(_!==t[f+g]||_!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let d=a*i,f=o*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Wn.prototype.ValueTypeName="";Wn.prototype.TimeBufferType=Float32Array;Wn.prototype.ValueBufferType=Float32Array;Wn.prototype.DefaultInterpolation=jf;var is=class extends Wn{constructor(e,t,i){super(e,t,i)}};is.prototype.ValueTypeName="bool";is.prototype.ValueBufferType=Array;is.prototype.DefaultInterpolation=cl;is.prototype.InterpolantFactoryMethodLinear=void 0;is.prototype.InterpolantFactoryMethodSmooth=void 0;var ch=class extends Wn{constructor(e,t,i,r){super(e,t,i,r)}};ch.prototype.ValueTypeName="color";var lh=class extends Wn{constructor(e,t,i,r){super(e,t,i,r)}};lh.prototype.ValueTypeName="number";var uh=class extends ns{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)zn.slerpFlat(s,0,o,l-a,o,l,c);return s}},wl=class extends Wn{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new uh(this.times,this.values,this.getValueSize(),e)}};wl.prototype.ValueTypeName="quaternion";wl.prototype.InterpolantFactoryMethodSmooth=void 0;var rs=class extends Wn{constructor(e,t,i){super(e,t,i)}};rs.prototype.ValueTypeName="string";rs.prototype.ValueBufferType=Array;rs.prototype.DefaultInterpolation=cl;rs.prototype.InterpolantFactoryMethodLinear=void 0;rs.prototype.InterpolantFactoryMethodSmooth=void 0;var dh=class extends Wn{constructor(e,t,i,r){super(e,t,i,r)}};dh.prototype.ValueTypeName="vector";var fh=class extends qi{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new We(e),this.intensity=t}dispose(){this.dispatchEvent({type:"dispose"})}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}};var If=new P,Rf=new zn,ki=new P,Cl=class extends qi{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Rt,this.projectionMatrix=new Rt,this.projectionMatrixInverse=new Rt,this.coordinateSystem=yi,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(If,Rf,ki),ki.x===1&&ki.y===1&&ki.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(If,Rf,ki.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(If,Rf,ki),ki.x===1&&ki.y===1&&ki.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(If,Rf,ki.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},Kr=new P,LS=new Ne,FS=new Ne,gn=class extends Cl{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Ma*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(ol*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Ma*2*Math.atan(Math.tan(ol*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){Kr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(Kr.x,Kr.y).multiplyScalar(-e/Kr.z),Kr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(Kr.x,Kr.y).multiplyScalar(-e/Kr.z)}getViewSize(e,t){return this.getViewBounds(e,LS,FS),t.subVectors(FS,LS)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(ol*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var Dl=class extends Cl{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}};var Tl=class extends fh{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var va=-90,ya=1,hh=class extends qi{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new gn(va,ya,e,t);r.layers=this.layers,this.add(r);let s=new gn(va,ya,e,t);s.layers=this.layers,this.add(s);let o=new gn(va,ya,e,t);o.layers=this.layers,this.add(o);let a=new gn(va,ya,e,t);a.layers=this.layers,this.add(a);let c=new gn(va,ya,e,t);c.layers=this.layers,this.add(c);let l=new gn(va,ya,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===yi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===ul)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let _=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=_,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},ph=class extends gn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var f_="\\[\\]\\.:\\/",WP=new RegExp("["+f_+"]","g"),h_="[^"+f_+"]",$P="[^"+f_.replace("\\.","")+"]",qP=/((?:WC+[\/:])*)/.source.replace("WC",h_),XP=/(WCOD+)?/.source.replace("WCOD",$P),YP=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",h_),ZP=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",h_),KP=new RegExp("^"+qP+XP+YP+ZP+"$"),JP=["material","materials","bones","map"],Hy=class{constructor(e,t,i){let r=i||It.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},It=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(WP,"")}static parseTrackName(t){let i=KP.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);JP.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Ae("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Re("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Re("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Re("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Re("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Re("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Re("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Re("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Re("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Re("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Re("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Hy,n})();It.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};It.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};It.prototype.GetterByBindingType=[It.prototype._getValue_direct,It.prototype._getValue_array,It.prototype._getValue_arrayElement,It.prototype._getValue_toArray];It.prototype.SetterByBindingTypeAndVersioning=[[It.prototype._setValue_direct,It.prototype._setValue_direct_setNeedsUpdate,It.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[It.prototype._setValue_array,It.prototype._setValue_array_setNeedsUpdate,It.prototype._setValue_array_setMatrixWorldNeedsUpdate],[It.prototype._setValue_arrayElement,It.prototype._setValue_arrayElement_setNeedsUpdate,It.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[It.prototype._setValue_fromArray,It.prototype._setValue_fromArray_setNeedsUpdate,It.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var EX=new Float32Array(1);var Aa=class{constructor(e=1,t=0,i=0){this.radius=e,this.phi=t,this.theta=i}set(e,t,i){return this.radius=e,this.phi=t,this.theta=i,this}copy(e){return this.radius=e.radius,this.phi=e.phi,this.theta=e.theta,this}makeSafe(){return this.phi=Qe(this.phi,1e-6,Math.PI-1e-6),this}setFromVector3(e){return this.setFromCartesianCoords(e.x,e.y,e.z)}setFromCartesianCoords(e,t,i){return this.radius=Math.sqrt(e*e+t*t+i*i),this.radius===0?(this.theta=0,this.phi=0):(this.theta=Math.atan2(e,i),this.phi=Math.acos(Qe(t/this.radius,-1,1))),this}clone(){return new this.constructor().copy(this)}};var Al=class extends Qf{constructor(e=1){let t=[0,0,0,e,0,0,0,0,0,0,e,0,0,0,0,0,0,e],i=[1,0,0,1,.6,0,0,1,0,.6,1,0,0,0,1,0,.6,1],r=new vn;r.setAttribute("position",new jt(t,3)),r.setAttribute("color",new jt(i,3));let s=new io({vertexColors:!0,toneMapped:!1});super(r,s),this.type="AxesHelper"}setColors(e,t,i){let r=new We,s=this.geometry.attributes.color.array;return r.set(e),r.toArray(s,0),r.toArray(s,3),r.set(t),r.toArray(s,6),r.toArray(s,9),r.set(i),r.toArray(s,12),r.toArray(s,15),this.geometry.attributes.color.needsUpdate=!0,this}dispose(){this.geometry.dispose(),this.material.dispose()}};var Il=class extends Hi{constructor(e,t=null){super(),this.object=e,this.domElement=t,this.enabled=!0,this.state=-1,this.keys={},this.mouseButtons={LEFT:null,MIDDLE:null,RIGHT:null},this.touches={ONE:null,TWO:null}}connect(e){if(e===void 0){Ae("Controls: connect() now requires an element.");return}this.domElement!==null&&this.disconnect(),this.domElement=e}disconnect(){}dispose(){}update(){}};function p_(n,e,t,i){let r=QP(i);switch(t){case r_:return n*e;case o_:return n*e/r.components*r.byteLength;case Mh:return n*e/r.components*r.byteLength;case so:return n*e*2/r.components*r.byteLength;case bh:return n*e*2/r.components*r.byteLength;case s_:return n*e*3/r.components*r.byteLength;case ii:return n*e*4/r.components*r.byteLength;case Eh:return n*e*4/r.components*r.byteLength;case Ol:case Ll:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Fl:case kl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case wh:case Dh:return Math.max(n,16)*Math.max(e,8)/4;case Sh:case Ch:return Math.max(n,8)*Math.max(e,8)/2;case Th:case Ah:case Rh:case Nh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Ih:case Ph:case Oh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Lh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Fh:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case kh:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Uh:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Bh:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Vh:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Hh:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case zh:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Gh:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case jh:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Wh:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case $h:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case qh:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Xh:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Yh:case Zh:case Kh:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Jh:case Qh:return Math.ceil(n/4)*Math.ceil(e/4)*8;case ep:case tp:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function QP(n){switch(n){case $n:case e_:return{byteLength:1,components:1};case Ra:case t_:case Wi:return{byteLength:2,components:1};case _h:case xh:return{byteLength:2,components:4};case xi:case yh:case Mi:return{byteLength:4,components:1};case n_:case i_:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"183"}}));typeof window<"u"&&(window.__THREE__?Ae("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="183");function jw(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function tO(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,f=n.createBuffer();n.bindBuffer(c,f),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:f,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((h,g)=>h.start-g.start);let f=0;for(let h=1;h<d.length;h++){let g=d[f],_=d[h];_.start<=g.start+g.count+1?g.count=Math.max(g.count,_.start+_.count-g.start):(++f,d[f]=_)}d.length=f+1;for(let h=0,g=d.length;h<g;h++){let _=d[h];n.bufferSubData(l,_.start*u.BYTES_PER_ELEMENT,u,_.start,_.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var nO=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,iO=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,rO=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,sO=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,oO=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,aO=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,cO=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,lO=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,uO=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,dO=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,fO=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,hO=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,pO=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,mO=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,gO=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,vO=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,yO=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,_O=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,xO=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,MO=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,bO=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,EO=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,SO=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,wO=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,CO=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,DO=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,TO=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,AO=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,IO=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,RO=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,NO="gl_FragColor = linearToOutputTexel( gl_FragColor );",PO=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,OO=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * vec3( flipEnvMap * reflectVec.x, reflectVec.yz ) );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,LO=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,FO=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,kO=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,UO=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,BO=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,VO=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,HO=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,zO=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,GO=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,jO=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,WO=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,$O=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,qO=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif`,XO=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,YO=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,ZO=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,KO=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,JO=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,QO=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,eL=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		float v = 0.5 / ( gv + gl );
		return v;
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,tL=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,nL=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,iL=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,rL=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,sL=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,oL=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,aL=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,cL=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,lL=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,uL=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,dL=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,fL=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,hL=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,pL=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,mL=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,gL=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,vL=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,yL=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,_L=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,xL=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,ML=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,bL=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,EL=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,SL=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,wL=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,CL=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,DL=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,TL=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,AL=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,IL=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,RL=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,NL=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,PL=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,OL=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,LL=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,FL=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,kL=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,UL=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,BL=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,VL=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,HL=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,zL=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,GL=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,jL=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,WL=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,$L=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,qL=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,XL=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,YL=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,ZL=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,KL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,JL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,QL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,eF=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,tF=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,nF=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,iF=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,rF=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float flipEnvMap;
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,sF=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,oF=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,aF=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,cF=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,lF=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,uF=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,dF=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,fF=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,hF=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,pF=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,mF=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,gF=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,vF=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,yF=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,_F=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,xF=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,MF=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,bF=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,EF=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,SF=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,wF=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,CF=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,DF=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,TF=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,AF=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,IF=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,RF=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,NF=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,PF=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,OF=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,Xe={alphahash_fragment:nO,alphahash_pars_fragment:iO,alphamap_fragment:rO,alphamap_pars_fragment:sO,alphatest_fragment:oO,alphatest_pars_fragment:aO,aomap_fragment:cO,aomap_pars_fragment:lO,batching_pars_vertex:uO,batching_vertex:dO,begin_vertex:fO,beginnormal_vertex:hO,bsdfs:pO,iridescence_fragment:mO,bumpmap_pars_fragment:gO,clipping_planes_fragment:vO,clipping_planes_pars_fragment:yO,clipping_planes_pars_vertex:_O,clipping_planes_vertex:xO,color_fragment:MO,color_pars_fragment:bO,color_pars_vertex:EO,color_vertex:SO,common:wO,cube_uv_reflection_fragment:CO,defaultnormal_vertex:DO,displacementmap_pars_vertex:TO,displacementmap_vertex:AO,emissivemap_fragment:IO,emissivemap_pars_fragment:RO,colorspace_fragment:NO,colorspace_pars_fragment:PO,envmap_fragment:OO,envmap_common_pars_fragment:LO,envmap_pars_fragment:FO,envmap_pars_vertex:kO,envmap_physical_pars_fragment:XO,envmap_vertex:UO,fog_vertex:BO,fog_pars_vertex:VO,fog_fragment:HO,fog_pars_fragment:zO,gradientmap_pars_fragment:GO,lightmap_pars_fragment:jO,lights_lambert_fragment:WO,lights_lambert_pars_fragment:$O,lights_pars_begin:qO,lights_toon_fragment:YO,lights_toon_pars_fragment:ZO,lights_phong_fragment:KO,lights_phong_pars_fragment:JO,lights_physical_fragment:QO,lights_physical_pars_fragment:eL,lights_fragment_begin:tL,lights_fragment_maps:nL,lights_fragment_end:iL,logdepthbuf_fragment:rL,logdepthbuf_pars_fragment:sL,logdepthbuf_pars_vertex:oL,logdepthbuf_vertex:aL,map_fragment:cL,map_pars_fragment:lL,map_particle_fragment:uL,map_particle_pars_fragment:dL,metalnessmap_fragment:fL,metalnessmap_pars_fragment:hL,morphinstance_vertex:pL,morphcolor_vertex:mL,morphnormal_vertex:gL,morphtarget_pars_vertex:vL,morphtarget_vertex:yL,normal_fragment_begin:_L,normal_fragment_maps:xL,normal_pars_fragment:ML,normal_pars_vertex:bL,normal_vertex:EL,normalmap_pars_fragment:SL,clearcoat_normal_fragment_begin:wL,clearcoat_normal_fragment_maps:CL,clearcoat_pars_fragment:DL,iridescence_pars_fragment:TL,opaque_fragment:AL,packing:IL,premultiplied_alpha_fragment:RL,project_vertex:NL,dithering_fragment:PL,dithering_pars_fragment:OL,roughnessmap_fragment:LL,roughnessmap_pars_fragment:FL,shadowmap_pars_fragment:kL,shadowmap_pars_vertex:UL,shadowmap_vertex:BL,shadowmask_pars_fragment:VL,skinbase_vertex:HL,skinning_pars_vertex:zL,skinning_vertex:GL,skinnormal_vertex:jL,specularmap_fragment:WL,specularmap_pars_fragment:$L,tonemapping_fragment:qL,tonemapping_pars_fragment:XL,transmission_fragment:YL,transmission_pars_fragment:ZL,uv_pars_fragment:KL,uv_pars_vertex:JL,uv_vertex:QL,worldpos_vertex:eF,background_vert:tF,background_frag:nF,backgroundCube_vert:iF,backgroundCube_frag:rF,cube_vert:sF,cube_frag:oF,depth_vert:aF,depth_frag:cF,distance_vert:lF,distance_frag:uF,equirect_vert:dF,equirect_frag:fF,linedashed_vert:hF,linedashed_frag:pF,meshbasic_vert:mF,meshbasic_frag:gF,meshlambert_vert:vF,meshlambert_frag:yF,meshmatcap_vert:_F,meshmatcap_frag:xF,meshnormal_vert:MF,meshnormal_frag:bF,meshphong_vert:EF,meshphong_frag:SF,meshphysical_vert:wF,meshphysical_frag:CF,meshtoon_vert:DF,meshtoon_frag:TF,points_vert:AF,points_frag:IF,shadow_vert:RF,shadow_frag:NF,sprite_vert:PF,sprite_frag:OF},ce={common:{diffuse:{value:new We(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new ze},alphaMap:{value:null},alphaMapTransform:{value:new ze},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new ze}},envmap:{envMap:{value:null},envMapRotation:{value:new ze},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new ze}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new ze}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new ze},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new ze},normalScale:{value:new Ne(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new ze},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new ze}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new ze}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new ze}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new We(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new We(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new ze},alphaTest:{value:0},uvTransform:{value:new ze}},sprite:{diffuse:{value:new We(16777215)},opacity:{value:1},center:{value:new Ne(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new ze},alphaMap:{value:null},alphaMapTransform:{value:new ze},alphaTest:{value:0}}},Yi={basic:{uniforms:yn([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.fog]),vertexShader:Xe.meshbasic_vert,fragmentShader:Xe.meshbasic_frag},lambert:{uniforms:yn([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,ce.lights,{emissive:{value:new We(0)},envMapIntensity:{value:1}}]),vertexShader:Xe.meshlambert_vert,fragmentShader:Xe.meshlambert_frag},phong:{uniforms:yn([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,ce.lights,{emissive:{value:new We(0)},specular:{value:new We(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:Xe.meshphong_vert,fragmentShader:Xe.meshphong_frag},standard:{uniforms:yn([ce.common,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.roughnessmap,ce.metalnessmap,ce.fog,ce.lights,{emissive:{value:new We(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Xe.meshphysical_vert,fragmentShader:Xe.meshphysical_frag},toon:{uniforms:yn([ce.common,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.gradientmap,ce.fog,ce.lights,{emissive:{value:new We(0)}}]),vertexShader:Xe.meshtoon_vert,fragmentShader:Xe.meshtoon_frag},matcap:{uniforms:yn([ce.common,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,{matcap:{value:null}}]),vertexShader:Xe.meshmatcap_vert,fragmentShader:Xe.meshmatcap_frag},points:{uniforms:yn([ce.points,ce.fog]),vertexShader:Xe.points_vert,fragmentShader:Xe.points_frag},dashed:{uniforms:yn([ce.common,ce.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Xe.linedashed_vert,fragmentShader:Xe.linedashed_frag},depth:{uniforms:yn([ce.common,ce.displacementmap]),vertexShader:Xe.depth_vert,fragmentShader:Xe.depth_frag},normal:{uniforms:yn([ce.common,ce.bumpmap,ce.normalmap,ce.displacementmap,{opacity:{value:1}}]),vertexShader:Xe.meshnormal_vert,fragmentShader:Xe.meshnormal_frag},sprite:{uniforms:yn([ce.sprite,ce.fog]),vertexShader:Xe.sprite_vert,fragmentShader:Xe.sprite_frag},background:{uniforms:{uvTransform:{value:new ze},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Xe.background_vert,fragmentShader:Xe.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new ze}},vertexShader:Xe.backgroundCube_vert,fragmentShader:Xe.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Xe.cube_vert,fragmentShader:Xe.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Xe.equirect_vert,fragmentShader:Xe.equirect_frag},distance:{uniforms:yn([ce.common,ce.displacementmap,{referencePosition:{value:new P},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Xe.distance_vert,fragmentShader:Xe.distance_frag},shadow:{uniforms:yn([ce.lights,ce.fog,{color:{value:new We(0)},opacity:{value:1}}]),vertexShader:Xe.shadow_vert,fragmentShader:Xe.shadow_frag}};Yi.physical={uniforms:yn([Yi.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new ze},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new ze},clearcoatNormalScale:{value:new Ne(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new ze},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new ze},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new ze},sheen:{value:0},sheenColor:{value:new We(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new ze},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new ze},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new ze},transmissionSamplerSize:{value:new Ne},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new ze},attenuationDistance:{value:0},attenuationColor:{value:new We(0)},specularColor:{value:new We(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new ze},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new ze},anisotropyVector:{value:new Ne},anisotropyMap:{value:null},anisotropyMapTransform:{value:new ze}}]),vertexShader:Xe.meshphysical_vert,fragmentShader:Xe.meshphysical_frag};var rp={r:0,b:0,g:0},ao=new Qr,LF=new Rt;function FF(n,e,t,i,r,s){let o=new We(0),a=r===!0?0:1,c,l,u=null,d=0,f=null;function h(M){let w=M.isScene===!0?M.background:null;if(w&&w.isTexture){let S=M.backgroundBlurriness>0;w=e.get(w,S)}return w}function g(M){let w=!1,S=h(M);S===null?m(o,a):S&&S.isColor&&(m(S,1),w=!0);let T=n.xr.getEnvironmentBlendMode();T==="additive"?t.buffers.color.setClear(0,0,0,1,s):T==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||w)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function _(M,w){let S=h(w);S&&(S.isCubeTexture||S.mapping===Nl)?(l===void 0&&(l=new wn(new Da(1,1,1),new jn({name:"BackgroundCubeMaterial",uniforms:oo(Yi.backgroundCube.uniforms),vertexShader:Yi.backgroundCube.vertexShader,fragmentShader:Yi.backgroundCube.fragmentShader,side:Cn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(T,D,I){this.matrixWorld.copyPosition(I.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),ao.copy(w.backgroundRotation),ao.x*=-1,ao.y*=-1,ao.z*=-1,S.isCubeTexture&&S.isRenderTargetTexture===!1&&(ao.y*=-1,ao.z*=-1),l.material.uniforms.envMap.value=S,l.material.uniforms.flipEnvMap.value=S.isCubeTexture&&S.isRenderTargetTexture===!1?-1:1,l.material.uniforms.backgroundBlurriness.value=w.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=w.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(LF.makeRotationFromEuler(ao)),l.material.toneMapped=nt.getTransfer(S.colorSpace)!==ft,(u!==S||d!==S.version||f!==n.toneMapping)&&(l.material.needsUpdate=!0,u=S,d=S.version,f=n.toneMapping),l.layers.enableAll(),M.unshift(l,l.geometry,l.material,0,0,null)):S&&S.isTexture&&(c===void 0&&(c=new wn(new El(2,2),new jn({name:"BackgroundMaterial",uniforms:oo(Yi.background.uniforms),vertexShader:Yi.background.vertexShader,fragmentShader:Yi.background.fragmentShader,side:Sr,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=S,c.material.uniforms.backgroundIntensity.value=w.backgroundIntensity,c.material.toneMapped=nt.getTransfer(S.colorSpace)!==ft,S.matrixAutoUpdate===!0&&S.updateMatrix(),c.material.uniforms.uvTransform.value.copy(S.matrix),(u!==S||d!==S.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,u=S,d=S.version,f=n.toneMapping),c.layers.enableAll(),M.unshift(c,c.geometry,c.material,0,0,null))}function m(M,w){M.getRGB(rp,d_(n)),t.buffers.color.setClear(rp.r,rp.g,rp.b,w,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(M,w=1){o.set(M),a=w,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(M){a=M,m(o,a)},render:g,addToRenderList:_,dispose:p}}function kF(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),s=r,o=!1;function a(C,U,V,j,B){let H=!1,L=d(C,j,V,U);s!==L&&(s=L,l(s.object)),H=h(C,j,V,B),H&&g(C,j,V,B),B!==null&&e.update(B,n.ELEMENT_ARRAY_BUFFER),(H||o)&&(o=!1,S(C,U,V,j),B!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(B).buffer))}function c(){return n.createVertexArray()}function l(C){return n.bindVertexArray(C)}function u(C){return n.deleteVertexArray(C)}function d(C,U,V,j){let B=j.wireframe===!0,H=i[U.id];H===void 0&&(H={},i[U.id]=H);let L=C.isInstancedMesh===!0?C.id:0,Q=H[L];Q===void 0&&(Q={},H[L]=Q);let Z=Q[V.id];Z===void 0&&(Z={},Q[V.id]=Z);let de=Z[B];return de===void 0&&(de=f(c()),Z[B]=de),de}function f(C){let U=[],V=[],j=[];for(let B=0;B<t;B++)U[B]=0,V[B]=0,j[B]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:U,enabledAttributes:V,attributeDivisors:j,object:C,attributes:{},index:null}}function h(C,U,V,j){let B=s.attributes,H=U.attributes,L=0,Q=V.getAttributes();for(let Z in Q)if(Q[Z].location>=0){let ge=B[Z],he=H[Z];if(he===void 0&&(Z==="instanceMatrix"&&C.instanceMatrix&&(he=C.instanceMatrix),Z==="instanceColor"&&C.instanceColor&&(he=C.instanceColor)),ge===void 0||ge.attribute!==he||he&&ge.data!==he.data)return!0;L++}return s.attributesNum!==L||s.index!==j}function g(C,U,V,j){let B={},H=U.attributes,L=0,Q=V.getAttributes();for(let Z in Q)if(Q[Z].location>=0){let ge=H[Z];ge===void 0&&(Z==="instanceMatrix"&&C.instanceMatrix&&(ge=C.instanceMatrix),Z==="instanceColor"&&C.instanceColor&&(ge=C.instanceColor));let he={};he.attribute=ge,ge&&ge.data&&(he.data=ge.data),B[Z]=he,L++}s.attributes=B,s.attributesNum=L,s.index=j}function _(){let C=s.newAttributes;for(let U=0,V=C.length;U<V;U++)C[U]=0}function m(C){p(C,0)}function p(C,U){let V=s.newAttributes,j=s.enabledAttributes,B=s.attributeDivisors;V[C]=1,j[C]===0&&(n.enableVertexAttribArray(C),j[C]=1),B[C]!==U&&(n.vertexAttribDivisor(C,U),B[C]=U)}function M(){let C=s.newAttributes,U=s.enabledAttributes;for(let V=0,j=U.length;V<j;V++)U[V]!==C[V]&&(n.disableVertexAttribArray(V),U[V]=0)}function w(C,U,V,j,B,H,L){L===!0?n.vertexAttribIPointer(C,U,V,B,H):n.vertexAttribPointer(C,U,V,j,B,H)}function S(C,U,V,j){_();let B=j.attributes,H=V.getAttributes(),L=U.defaultAttributeValues;for(let Q in H){let Z=H[Q];if(Z.location>=0){let de=B[Q];if(de===void 0&&(Q==="instanceMatrix"&&C.instanceMatrix&&(de=C.instanceMatrix),Q==="instanceColor"&&C.instanceColor&&(de=C.instanceColor)),de!==void 0){let ge=de.normalized,he=de.itemSize,$e=e.get(de);if($e===void 0)continue;let Ct=$e.buffer,wt=$e.type,X=$e.bytesPerElement,ne=wt===n.INT||wt===n.UNSIGNED_INT||de.gpuType===yh;if(de.isInterleavedBufferAttribute){let oe=de.data,Ge=oe.stride,Ie=de.offset;if(oe.isInstancedInterleavedBuffer){for(let Le=0;Le<Z.locationSize;Le++)p(Z.location+Le,oe.meshPerAttribute);C.isInstancedMesh!==!0&&j._maxInstanceCount===void 0&&(j._maxInstanceCount=oe.meshPerAttribute*oe.count)}else for(let Le=0;Le<Z.locationSize;Le++)m(Z.location+Le);n.bindBuffer(n.ARRAY_BUFFER,Ct);for(let Le=0;Le<Z.locationSize;Le++)w(Z.location+Le,he/Z.locationSize,wt,ge,Ge*X,(Ie+he/Z.locationSize*Le)*X,ne)}else{if(de.isInstancedBufferAttribute){for(let oe=0;oe<Z.locationSize;oe++)p(Z.location+oe,de.meshPerAttribute);C.isInstancedMesh!==!0&&j._maxInstanceCount===void 0&&(j._maxInstanceCount=de.meshPerAttribute*de.count)}else for(let oe=0;oe<Z.locationSize;oe++)m(Z.location+oe);n.bindBuffer(n.ARRAY_BUFFER,Ct);for(let oe=0;oe<Z.locationSize;oe++)w(Z.location+oe,he/Z.locationSize,wt,ge,he*X,he/Z.locationSize*oe*X,ne)}}else if(L!==void 0){let ge=L[Q];if(ge!==void 0)switch(ge.length){case 2:n.vertexAttrib2fv(Z.location,ge);break;case 3:n.vertexAttrib3fv(Z.location,ge);break;case 4:n.vertexAttrib4fv(Z.location,ge);break;default:n.vertexAttrib1fv(Z.location,ge)}}}}M()}function T(){E();for(let C in i){let U=i[C];for(let V in U){let j=U[V];for(let B in j){let H=j[B];for(let L in H)u(H[L].object),delete H[L];delete j[B]}}delete i[C]}}function D(C){if(i[C.id]===void 0)return;let U=i[C.id];for(let V in U){let j=U[V];for(let B in j){let H=j[B];for(let L in H)u(H[L].object),delete H[L];delete j[B]}}delete i[C.id]}function I(C){for(let U in i){let V=i[U];for(let j in V){let B=V[j];if(B[C.id]===void 0)continue;let H=B[C.id];for(let L in H)u(H[L].object),delete H[L];delete B[C.id]}}}function y(C){for(let U in i){let V=i[U],j=C.isInstancedMesh===!0?C.id:0,B=V[j];if(B!==void 0){for(let H in B){let L=B[H];for(let Q in L)u(L[Q].object),delete L[Q];delete B[H]}delete V[j],Object.keys(V).length===0&&delete i[U]}}}function E(){q(),o=!0,s!==r&&(s=r,l(s.object))}function q(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:E,resetDefaultState:q,dispose:T,releaseStatesOfGeometry:D,releaseStatesOfObject:y,releaseStatesOfProgram:I,initAttributes:_,enableAttribute:m,disableUnusedAttributes:M}}function UF(n,e,t){let i;function r(l){i=l}function s(l,u){n.drawArrays(i,l,u),t.update(u,i,1)}function o(l,u,d){d!==0&&(n.drawArraysInstanced(i,l,u,d),t.update(u,i,d))}function a(l,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,l,0,u,0,d);let h=0;for(let g=0;g<d;g++)h+=u[g];t.update(h,i,1)}function c(l,u,d,f){if(d===0)return;let h=e.get("WEBGL_multi_draw");if(h===null)for(let g=0;g<l.length;g++)o(l[g],u[g],f[g]);else{h.multiDrawArraysInstancedWEBGL(i,l,0,u,0,f,0,d);let g=0;for(let _=0;_<d;_++)g+=u[_]*f[_];t.update(g,i,1)}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=c}function BF(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let I=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(I.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(I){return!(I!==ii&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(I){let y=I===Wi&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(I!==$n&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&I!==Mi&&!y)}function c(I){if(I==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";I="mediump"}return I==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Ae("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,f=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control"),h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),_=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),M=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),w=n.getParameter(n.MAX_VARYING_VECTORS),S=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),T=n.getParameter(n.MAX_SAMPLES),D=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reversedDepthBuffer:f,maxTextures:h,maxVertexTextures:g,maxTextureSize:_,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:M,maxVaryings:w,maxFragmentUniforms:S,maxSamples:T,samples:D}}function VF(n){let e=this,t=null,i=0,r=!1,s=!1,o=new ni,a=new ze,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let g=d.clippingPlanes,_=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let M=s?0:i,w=M*4,S=p.clippingState||null;c.value=S,S=u(g,f,w,h);for(let T=0;T!==w;++T)S[T]=t[T];p.clippingState=S,this.numIntersection=_?this.numPlanes:0,this.numPlanes+=M}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,g){let _=d!==null?d.length:0,m=null;if(_!==0){if(m=c.value,g!==!0||m===null){let p=h+_*4,M=f.matrixWorldInverse;a.getNormalMatrix(M),(m===null||m.length<p)&&(m=new Float32Array(p));for(let w=0,S=h;w!==_;++w,S+=4)o.copy(d[w]).applyMatrix4(M,a),o.normal.toArray(m,S),m[S+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=_,e.numIntersection=0,m}}var us=4,bw=[.125,.215,.35,.446,.526,.582],lo=20,HF=256,Ul=new Dl,Ew=new We,m_=null,g_=0,v_=0,y_=!1,zF=new P,op=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=zF}=s;m_=this._renderer.getRenderTarget(),g_=this._renderer.getActiveCubeFace(),v_=this._renderer.getActiveMipmapLevel(),y_=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=Cw(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=ww(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(m_,g_,v_),this._renderer.xr.enabled=y_,e.scissorTest=!1,Pa(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===as||e.mapping===ro?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),m_=this._renderer.getRenderTarget(),g_=this._renderer.getActiveCubeFace(),v_=this._renderer.getActiveMipmapLevel(),y_=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:cn,minFilter:cn,generateMipmaps:!1,type:Wi,format:ii,colorSpace:Js,depthBuffer:!1},r=Sw(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=Sw(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=GF(s)),this._blurMaterial=WF(s,e,t),this._ggxMaterial=jF(s,e,t)}return r}_compileMaterial(e){let t=new wn(new vn,e);this._renderer.compile(t,Ul)}_sceneToCubeUV(e,t,i,r,s){let c=new gn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],d=this._renderer,f=d.autoClear,h=d.toneMapping;d.getClearColor(Ew),d.toneMapping=_i,d.autoClear=!1,d.state.buffers.depth.getReversed()&&(d.setRenderTarget(r),d.clearDepth(),d.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new wn(new Da,new no({name:"PMREM.Background",side:Cn,depthWrite:!1,depthTest:!1})));let _=this._backgroundBox,m=_.material,p=!1,M=e.background;M?M.isColor&&(m.color.copy(M),e.background=null,p=!0):(m.color.copy(Ew),p=!0);for(let w=0;w<6;w++){let S=w%3;S===0?(c.up.set(0,l[w],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[w],s.y,s.z)):S===1?(c.up.set(0,0,l[w]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[w],s.z)):(c.up.set(0,l[w],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[w]));let T=this._cubeSize;Pa(r,S*T,w>2?T:0,T,T),d.setRenderTarget(r),p&&d.render(_,c),d.render(e,c)}d.toneMapping=h,d.autoClear=f,e.background=M}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===as||e.mapping===ro;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=Cw()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=ww());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Pa(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,Ul)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),d=Math.sqrt(l*l-u*u),f=0+l*1.25,h=d*f,{_lodMax:g}=this,_=this._sizeLods[i],m=3*_*(i>g-us?i-g+us:0),p=4*(this._cubeSize-_);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=g-t,Pa(s,m,p,3*_,2*_),r.setRenderTarget(s),r.render(a,Ul),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=g-i,Pa(e,m,p,3*_,2*_),r.setRenderTarget(e),r.render(a,Ul)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&Re("blur direction must be either latitudinal or longitudinal!");let u=3,d=this._lodMeshes[r];d.material=l;let f=l.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*lo-1),_=s/g,m=isFinite(s)?1+Math.floor(u*_):lo;m>lo&&Ae(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${lo}`);let p=[],M=0;for(let I=0;I<lo;++I){let y=I/_,E=Math.exp(-y*y/2);p.push(E),I===0?M+=E:I<m&&(M+=2*E)}for(let I=0;I<p.length;I++)p[I]=p[I]/M;f.envMap.value=e.texture,f.samples.value=m,f.weights.value=p,f.latitudinal.value=o==="latitudinal",a&&(f.poleAxis.value=a);let{_lodMax:w}=this;f.dTheta.value=g,f.mipInt.value=w-i;let S=this._sizeLods[r],T=3*S*(r>w-us?r-w+us:0),D=4*(this._cubeSize-S);Pa(t,T,D,3*S,2*S),c.setRenderTarget(t),c.render(d,Ul)}};function GF(n){let e=[],t=[],i=[],r=n,s=n-us+1+bw.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-us?c=bw[o-n+us-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,d=1+l,f=[u,u,d,u,d,d,u,u,d,d,u,d],h=6,g=6,_=3,m=2,p=1,M=new Float32Array(_*g*h),w=new Float32Array(m*g*h),S=new Float32Array(p*g*h);for(let D=0;D<h;D++){let I=D%3*2/3-1,y=D>2?0:-1,E=[I,y,0,I+2/3,y,0,I+2/3,y+1,0,I,y,0,I+2/3,y+1,0,I,y+1,0];M.set(E,_*g*D),w.set(f,m*g*D);let q=[D,D,D,D,D,D];S.set(q,p*g*D)}let T=new vn;T.setAttribute("position",new An(M,_)),T.setAttribute("uv",new An(w,m)),T.setAttribute("faceIndex",new An(S,p)),i.push(new wn(T,null)),r>us&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function Sw(n,e,t){let i=new Gn(n,e,t);return i.texture.mapping=Nl,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Pa(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function jF(n,e,t){return new jn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:HF,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:lp(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:ji,depthTest:!1,depthWrite:!1})}function WF(n,e,t){let i=new Float32Array(lo),r=new P(0,1,0);return new jn({name:"SphericalGaussianBlur",defines:{n:lo,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:lp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:ji,depthTest:!1,depthWrite:!1})}function ww(){return new jn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:lp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:ji,depthTest:!1,depthWrite:!1})}function Cw(){return new jn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:lp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:ji,depthTest:!1,depthWrite:!1})}function lp(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var ap=class extends Gn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Ml(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new Da(5,5,5),s=new jn({name:"CubemapFromEquirect",uniforms:oo(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:Cn,blending:ji});s.uniforms.tEquirect.value=t;let o=new wn(r,s),a=t.minFilter;return t.minFilter===cs&&(t.minFilter=cn),new hh(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function $F(n){let e=new WeakMap,t=new WeakMap,i=null;function r(f,h=!1){return f==null?null:h?o(f):s(f)}function s(f){if(f&&f.isTexture){let h=f.mapping;if(h===mh||h===gh)if(e.has(f)){let g=e.get(f).texture;return a(g,f.mapping)}else{let g=f.image;if(g&&g.height>0){let _=new ap(g.height);return _.fromEquirectangularTexture(n,f),e.set(f,_),f.addEventListener("dispose",l),a(_.texture,f.mapping)}else return null}}return f}function o(f){if(f&&f.isTexture){let h=f.mapping,g=h===mh||h===gh,_=h===as||h===ro;if(g||_){let m=t.get(f),p=m!==void 0?m.texture.pmremVersion:0;if(f.isRenderTargetTexture&&f.pmremVersion!==p)return i===null&&(i=new op(n)),m=g?i.fromEquirectangular(f,m):i.fromCubemap(f,m),m.texture.pmremVersion=f.pmremVersion,t.set(f,m),m.texture;if(m!==void 0)return m.texture;{let M=f.image;return g&&M&&M.height>0||_&&M&&c(M)?(i===null&&(i=new op(n)),m=g?i.fromEquirectangular(f):i.fromCubemap(f),m.texture.pmremVersion=f.pmremVersion,t.set(f,m),f.addEventListener("dispose",u),m.texture):null}}}return f}function a(f,h){return h===mh?f.mapping=as:h===gh&&(f.mapping=ro),f}function c(f){let h=0,g=6;for(let _=0;_<g;_++)f[_]!==void 0&&h++;return h===g}function l(f){let h=f.target;h.removeEventListener("dispose",l);let g=e.get(h);g!==void 0&&(e.delete(h),g.dispose())}function u(f){let h=f.target;h.removeEventListener("dispose",u);let g=t.get(h);g!==void 0&&(t.delete(h),g.dispose())}function d(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:d}}function qF(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&hl("WebGLRenderer: "+i+" extension not supported."),r}}}function XF(n,e,t,i){let r={},s=new WeakMap;function o(d){let f=d.target;f.index!==null&&e.remove(f.index);for(let g in f.attributes)e.remove(f.attributes[g]);f.removeEventListener("dispose",o),delete r[f.id];let h=s.get(f);h&&(e.remove(h),s.delete(f)),i.releaseStatesOfGeometry(f),f.isInstancedBufferGeometry===!0&&delete f._maxInstanceCount,t.memory.geometries--}function a(d,f){return r[f.id]===!0||(f.addEventListener("dispose",o),r[f.id]=!0,t.memory.geometries++),f}function c(d){let f=d.attributes;for(let h in f)e.update(f[h],n.ARRAY_BUFFER)}function l(d){let f=[],h=d.index,g=d.attributes.position,_=0;if(g===void 0)return;if(h!==null){let M=h.array;_=h.version;for(let w=0,S=M.length;w<S;w+=3){let T=M[w+0],D=M[w+1],I=M[w+2];f.push(T,D,D,I,I,T)}}else{let M=g.array;_=g.version;for(let w=0,S=M.length/3-1;w<S;w+=3){let T=w+0,D=w+1,I=w+2;f.push(T,D,D,I,I,T)}}let m=new(g.count>=65535?yl:vl)(f,1);m.version=_;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let f=s.get(d);if(f){let h=d.index;h!==null&&f.version<h.version&&l(d)}else l(d);return s.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function YF(n,e,t){let i;function r(f){i=f}let s,o;function a(f){s=f.type,o=f.bytesPerElement}function c(f,h){n.drawElements(i,h,s,f*o),t.update(h,i,1)}function l(f,h,g){g!==0&&(n.drawElementsInstanced(i,h,s,f*o,g),t.update(h,i,g))}function u(f,h,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,h,0,s,f,0,g);let m=0;for(let p=0;p<g;p++)m+=h[p];t.update(m,i,1)}function d(f,h,g,_){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<f.length;p++)l(f[p]/o,h[p],_[p]);else{m.multiDrawElementsInstancedWEBGL(i,h,0,s,f,0,_,0,g);let p=0;for(let M=0;M<g;M++)p+=h[M]*_[M];t.update(p,i,1)}}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function ZF(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Re("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function KF(n,e,t){let i=new WeakMap,r=new Nt;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let q=function(){y.dispose(),i.delete(a),a.removeEventListener("dispose",q)};var h=q;f!==void 0&&f.texture.dispose();let g=a.morphAttributes.position!==void 0,_=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],M=a.morphAttributes.normal||[],w=a.morphAttributes.color||[],S=0;g===!0&&(S=1),_===!0&&(S=2),m===!0&&(S=3);let T=a.attributes.position.count*S,D=1;T>e.maxTextureSize&&(D=Math.ceil(T/e.maxTextureSize),T=e.maxTextureSize);let I=new Float32Array(T*D*4*d),y=new pl(I,T,D,d);y.type=Mi,y.needsUpdate=!0;let E=S*4;for(let C=0;C<d;C++){let U=p[C],V=M[C],j=w[C],B=T*D*4*C;for(let H=0;H<U.count;H++){let L=H*E;g===!0&&(r.fromBufferAttribute(U,H),I[B+L+0]=r.x,I[B+L+1]=r.y,I[B+L+2]=r.z,I[B+L+3]=0),_===!0&&(r.fromBufferAttribute(V,H),I[B+L+4]=r.x,I[B+L+5]=r.y,I[B+L+6]=r.z,I[B+L+7]=0),m===!0&&(r.fromBufferAttribute(j,H),I[B+L+8]=r.x,I[B+L+9]=r.y,I[B+L+10]=r.z,I[B+L+11]=j.itemSize===4?r.w:1)}}f={count:d,texture:y,size:new Ne(T,D)},i.set(a,f),a.addEventListener("dispose",q)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let _=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",_),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:s}}function JF(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,d=l.geometry,f=e.get(l,d);if(s.get(f)!==u&&(e.update(f),s.set(f,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return f}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var QF={[qy]:"LINEAR_TONE_MAPPING",[Xy]:"REINHARD_TONE_MAPPING",[Yy]:"CINEON_TONE_MAPPING",[Zy]:"ACES_FILMIC_TONE_MAPPING",[Jy]:"AGX_TONE_MAPPING",[Qy]:"NEUTRAL_TONE_MAPPING",[Ky]:"CUSTOM_TONE_MAPPING"};function ek(n,e,t,i,r){let s=new Gn(e,t,{type:n,depthBuffer:i,stencilBuffer:r}),o=new Gn(e,t,{type:Wi,depthBuffer:!1,stencilBuffer:!1}),a=new vn;a.setAttribute("position",new jt([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new jt([0,2,0,0,2,0],2));let c=new th({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new wn(a,c),u=new Dl(-1,1,1,-1,0,1),d=null,f=null,h=!1,g,_=null,m=[],p=!1;this.setSize=function(M,w){s.setSize(M,w),o.setSize(M,w);for(let S=0;S<m.length;S++){let T=m[S];T.setSize&&T.setSize(M,w)}},this.setEffects=function(M){m=M,p=m.length>0&&m[0].isRenderPass===!0;let w=s.width,S=s.height;for(let T=0;T<m.length;T++){let D=m[T];D.setSize&&D.setSize(w,S)}},this.begin=function(M,w){if(h||M.toneMapping===_i&&m.length===0)return!1;if(_=w,w!==null){let S=w.width,T=w.height;(s.width!==S||s.height!==T)&&this.setSize(S,T)}return p===!1&&M.setRenderTarget(s),g=M.toneMapping,M.toneMapping=_i,!0},this.hasRenderPass=function(){return p},this.end=function(M,w){M.toneMapping=g,h=!0;let S=s,T=o;for(let D=0;D<m.length;D++){let I=m[D];if(I.enabled!==!1&&(I.render(M,T,S,w),I.needsSwap!==!1)){let y=S;S=T,T=y}}if(d!==M.outputColorSpace||f!==M.toneMapping){d=M.outputColorSpace,f=M.toneMapping,c.defines={},nt.getTransfer(d)===ft&&(c.defines.SRGB_TRANSFER="");let D=QF[f];D&&(c.defines[D]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=S.texture,M.setRenderTarget(_),M.render(l,u),_=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var Ww=new $i,M_=new ts(1,1),$w=new pl,qw=new Xf,Xw=new Ml,Dw=[],Tw=[],Aw=new Float32Array(16),Iw=new Float32Array(9),Rw=new Float32Array(4);function La(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=Dw[r];if(s===void 0&&(s=new Float32Array(r),Dw[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Wt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function $t(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function up(n,e){let t=Tw[e];t===void 0&&(t=new Int32Array(e),Tw[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function tk(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function nk(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Wt(t,e))return;n.uniform2fv(this.addr,e),$t(t,e)}}function ik(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Wt(t,e))return;n.uniform3fv(this.addr,e),$t(t,e)}}function rk(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Wt(t,e))return;n.uniform4fv(this.addr,e),$t(t,e)}}function sk(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Wt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),$t(t,e)}else{if(Wt(t,i))return;Rw.set(i),n.uniformMatrix2fv(this.addr,!1,Rw),$t(t,i)}}function ok(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Wt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),$t(t,e)}else{if(Wt(t,i))return;Iw.set(i),n.uniformMatrix3fv(this.addr,!1,Iw),$t(t,i)}}function ak(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Wt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),$t(t,e)}else{if(Wt(t,i))return;Aw.set(i),n.uniformMatrix4fv(this.addr,!1,Aw),$t(t,i)}}function ck(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function lk(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Wt(t,e))return;n.uniform2iv(this.addr,e),$t(t,e)}}function uk(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Wt(t,e))return;n.uniform3iv(this.addr,e),$t(t,e)}}function dk(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Wt(t,e))return;n.uniform4iv(this.addr,e),$t(t,e)}}function fk(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function hk(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Wt(t,e))return;n.uniform2uiv(this.addr,e),$t(t,e)}}function pk(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Wt(t,e))return;n.uniform3uiv(this.addr,e),$t(t,e)}}function mk(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Wt(t,e))return;n.uniform4uiv(this.addr,e),$t(t,e)}}function gk(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(M_.compareFunction=t.isReversedDepthBuffer()?ip:np,s=M_):s=Ww,t.setTexture2D(e||s,r)}function vk(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||qw,r)}function yk(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||Xw,r)}function _k(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||$w,r)}function xk(n){switch(n){case 5126:return tk;case 35664:return nk;case 35665:return ik;case 35666:return rk;case 35674:return sk;case 35675:return ok;case 35676:return ak;case 5124:case 35670:return ck;case 35667:case 35671:return lk;case 35668:case 35672:return uk;case 35669:case 35673:return dk;case 5125:return fk;case 36294:return hk;case 36295:return pk;case 36296:return mk;case 35678:case 36198:case 36298:case 36306:case 35682:return gk;case 35679:case 36299:case 36307:return vk;case 35680:case 36300:case 36308:case 36293:return yk;case 36289:case 36303:case 36311:case 36292:return _k}}function Mk(n,e){n.uniform1fv(this.addr,e)}function bk(n,e){let t=La(e,this.size,2);n.uniform2fv(this.addr,t)}function Ek(n,e){let t=La(e,this.size,3);n.uniform3fv(this.addr,t)}function Sk(n,e){let t=La(e,this.size,4);n.uniform4fv(this.addr,t)}function wk(n,e){let t=La(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function Ck(n,e){let t=La(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function Dk(n,e){let t=La(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function Tk(n,e){n.uniform1iv(this.addr,e)}function Ak(n,e){n.uniform2iv(this.addr,e)}function Ik(n,e){n.uniform3iv(this.addr,e)}function Rk(n,e){n.uniform4iv(this.addr,e)}function Nk(n,e){n.uniform1uiv(this.addr,e)}function Pk(n,e){n.uniform2uiv(this.addr,e)}function Ok(n,e){n.uniform3uiv(this.addr,e)}function Lk(n,e){n.uniform4uiv(this.addr,e)}function Fk(n,e,t){let i=this.cache,r=e.length,s=up(t,r);Wt(i,s)||(n.uniform1iv(this.addr,s),$t(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=M_:o=Ww;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function kk(n,e,t){let i=this.cache,r=e.length,s=up(t,r);Wt(i,s)||(n.uniform1iv(this.addr,s),$t(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||qw,s[o])}function Uk(n,e,t){let i=this.cache,r=e.length,s=up(t,r);Wt(i,s)||(n.uniform1iv(this.addr,s),$t(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||Xw,s[o])}function Bk(n,e,t){let i=this.cache,r=e.length,s=up(t,r);Wt(i,s)||(n.uniform1iv(this.addr,s),$t(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||$w,s[o])}function Vk(n){switch(n){case 5126:return Mk;case 35664:return bk;case 35665:return Ek;case 35666:return Sk;case 35674:return wk;case 35675:return Ck;case 35676:return Dk;case 5124:case 35670:return Tk;case 35667:case 35671:return Ak;case 35668:case 35672:return Ik;case 35669:case 35673:return Rk;case 5125:return Nk;case 36294:return Pk;case 36295:return Ok;case 36296:return Lk;case 35678:case 36198:case 36298:case 36306:case 35682:return Fk;case 35679:case 36299:case 36307:return kk;case 35680:case 36300:case 36308:case 36293:return Uk;case 36289:case 36303:case 36311:case 36292:return Bk}}var b_=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=xk(t.type)}},E_=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=Vk(t.type)}},S_=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},__=/(\w+)(\])?(\[|\.)?/g;function Nw(n,e){n.seq.push(e),n.map[e.id]=e}function Hk(n,e,t){let i=n.name,r=i.length;for(__.lastIndex=0;;){let s=__.exec(i),o=__.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){Nw(t,l===void 0?new b_(a,n,e):new E_(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new S_(a),Nw(t,d)),t=d}}}var Oa=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);Hk(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function Pw(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var zk=37297,Gk=0;function jk(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var Ow=new ze;function Wk(n){nt._getMatrix(Ow,nt.workingColorSpace,n);let e=`mat3( ${Ow.elements.map(t=>t.toFixed(4))} )`;switch(nt.getTransfer(n)){case ll:return[e,"LinearTransferOETF"];case ft:return[e,"sRGBTransferOETF"];default:return Ae("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function Lw(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+jk(n.getShaderSource(e),a)}else return s}function $k(n,e){let t=Wk(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var qk={[qy]:"Linear",[Xy]:"Reinhard",[Yy]:"Cineon",[Zy]:"ACESFilmic",[Jy]:"AgX",[Qy]:"Neutral",[Ky]:"Custom"};function Xk(n,e){let t=qk[e];return t===void 0?(Ae("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var sp=new P;function Yk(){nt.getLuminanceCoefficients(sp);let n=sp.x.toFixed(4),e=sp.y.toFixed(4),t=sp.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function Zk(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Vl).join(`
`)}function Kk(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function Jk(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function Vl(n){return n!==""}function Fw(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function kw(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var Qk=/^[ \t]*#include +<([\w\d./]+)>/gm;function w_(n){return n.replace(Qk,t2)}var e2=new Map;function t2(n,e){let t=Xe[e];if(t===void 0){let i=e2.get(e);if(i!==void 0)t=Xe[i],Ae('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return w_(t)}var n2=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function Uw(n){return n.replace(n2,i2)}function i2(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function Bw(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var r2={[Rl]:"SHADOWMAP_TYPE_PCF",[Ia]:"SHADOWMAP_TYPE_VSM"};function s2(n){return r2[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var o2={[as]:"ENVMAP_TYPE_CUBE",[ro]:"ENVMAP_TYPE_CUBE",[Nl]:"ENVMAP_TYPE_CUBE_UV"};function a2(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":o2[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var c2={[ro]:"ENVMAP_MODE_REFRACTION"};function l2(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":c2[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var u2={[$y]:"ENVMAP_BLENDING_MULTIPLY",[rw]:"ENVMAP_BLENDING_MIX",[sw]:"ENVMAP_BLENDING_ADD"};function d2(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":u2[n.combine]||"ENVMAP_BLENDING_NONE"}function f2(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function h2(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=s2(t),l=a2(t),u=l2(t),d=d2(t),f=f2(t),h=Zk(t),g=Kk(s),_=r.createProgram(),m,p,M=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Vl).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Vl).join(`
`),p.length>0&&(p+=`
`)):(m=[Bw(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Vl).join(`
`),p=[Bw(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",f?"#define CUBEUV_TEXEL_WIDTH "+f.texelWidth:"",f?"#define CUBEUV_TEXEL_HEIGHT "+f.texelHeight:"",f?"#define CUBEUV_MAX_MIP "+f.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==_i?"#define TONE_MAPPING":"",t.toneMapping!==_i?Xe.tonemapping_pars_fragment:"",t.toneMapping!==_i?Xk("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Xe.colorspace_pars_fragment,$k("linearToOutputTexel",t.outputColorSpace),Yk(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Vl).join(`
`)),o=w_(o),o=Fw(o,t),o=kw(o,t),a=w_(a),a=Fw(a,t),a=kw(a,t),o=Uw(o),a=Uw(a),t.isRawShaderMaterial!==!0&&(M=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===c_?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===c_?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let w=M+m+o,S=M+p+a,T=Pw(r,r.VERTEX_SHADER,w),D=Pw(r,r.FRAGMENT_SHADER,S);r.attachShader(_,T),r.attachShader(_,D),t.index0AttributeName!==void 0?r.bindAttribLocation(_,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(_,0,"position"),r.linkProgram(_);function I(C){if(n.debug.checkShaderErrors){let U=r.getProgramInfoLog(_)||"",V=r.getShaderInfoLog(T)||"",j=r.getShaderInfoLog(D)||"",B=U.trim(),H=V.trim(),L=j.trim(),Q=!0,Z=!0;if(r.getProgramParameter(_,r.LINK_STATUS)===!1)if(Q=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,_,T,D);else{let de=Lw(r,T,"vertex"),ge=Lw(r,D,"fragment");Re("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(_,r.VALIDATE_STATUS)+`

Material Name: `+C.name+`
Material Type: `+C.type+`

Program Info Log: `+B+`
`+de+`
`+ge)}else B!==""?Ae("WebGLProgram: Program Info Log:",B):(H===""||L==="")&&(Z=!1);Z&&(C.diagnostics={runnable:Q,programLog:B,vertexShader:{log:H,prefix:m},fragmentShader:{log:L,prefix:p}})}r.deleteShader(T),r.deleteShader(D),y=new Oa(r,_),E=Jk(r,_)}let y;this.getUniforms=function(){return y===void 0&&I(this),y};let E;this.getAttributes=function(){return E===void 0&&I(this),E};let q=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return q===!1&&(q=r.getProgramParameter(_,zk)),q},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(_),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=Gk++,this.cacheKey=e,this.usedTimes=1,this.program=_,this.vertexShader=T,this.fragmentShader=D,this}var p2=0,C_=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new D_(e),t.set(e,i)),i}},D_=class{constructor(e){this.id=p2++,this.code=e,this.usedTimes=0}};function m2(n,e,t,i,r,s){let o=new ml,a=new C_,c=new Set,l=[],u=new Map,d=i.logarithmicDepthBuffer,f=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function g(y){return c.add(y),y===0?"uv":`uv${y}`}function _(y,E,q,C,U){let V=C.fog,j=U.geometry,B=y.isMeshStandardMaterial||y.isMeshLambertMaterial||y.isMeshPhongMaterial?C.environment:null,H=y.isMeshStandardMaterial||y.isMeshLambertMaterial&&!y.envMap||y.isMeshPhongMaterial&&!y.envMap,L=e.get(y.envMap||B,H),Q=L&&L.mapping===Nl?L.image.height:null,Z=h[y.type];y.precision!==null&&(f=i.getMaxPrecision(y.precision),f!==y.precision&&Ae("WebGLProgram.getParameters:",y.precision,"not supported, using",f,"instead."));let de=j.morphAttributes.position||j.morphAttributes.normal||j.morphAttributes.color,ge=de!==void 0?de.length:0,he=0;j.morphAttributes.position!==void 0&&(he=1),j.morphAttributes.normal!==void 0&&(he=2),j.morphAttributes.color!==void 0&&(he=3);let $e,Ct,wt,X;if(Z){let pt=Yi[Z];$e=pt.vertexShader,Ct=pt.fragmentShader}else $e=y.vertexShader,Ct=y.fragmentShader,a.update(y),wt=a.getVertexShaderID(y),X=a.getFragmentShaderID(y);let ne=n.getRenderTarget(),oe=n.state.buffers.depth.getReversed(),Ge=U.isInstancedMesh===!0,Ie=U.isBatchedMesh===!0,Le=!!y.map,Xt=!!y.matcap,it=!!L,ht=!!y.aoMap,Mt=!!y.lightMap,Ye=!!y.bumpMap,Pt=!!y.normalMap,A=!!y.displacementMap,Ft=!!y.emissiveMap,at=!!y.metalnessMap,Et=!!y.roughnessMap,be=y.anisotropy>0,b=y.clearcoat>0,v=y.dispersion>0,N=y.iridescence>0,$=y.sheen>0,Y=y.transmission>0,W=be&&!!y.anisotropyMap,ve=b&&!!y.clearcoatMap,re=b&&!!y.clearcoatNormalMap,De=b&&!!y.clearcoatRoughnessMap,Pe=N&&!!y.iridescenceMap,K=N&&!!y.iridescenceThicknessMap,ee=$&&!!y.sheenColorMap,ye=$&&!!y.sheenRoughnessMap,xe=!!y.specularMap,fe=!!y.specularColorMap,Ze=!!y.specularIntensityMap,R=Y&&!!y.transmissionMap,se=Y&&!!y.thicknessMap,te=!!y.gradientMap,me=!!y.alphaMap,J=y.alphaTest>0,G=!!y.alphaHash,_e=!!y.extensions,ke=_i;y.toneMapped&&(ne===null||ne.isXRRenderTarget===!0)&&(ke=n.toneMapping);let St={shaderID:Z,shaderType:y.type,shaderName:y.name,vertexShader:$e,fragmentShader:Ct,defines:y.defines,customVertexShaderID:wt,customFragmentShaderID:X,isRawShaderMaterial:y.isRawShaderMaterial===!0,glslVersion:y.glslVersion,precision:f,batching:Ie,batchingColor:Ie&&U._colorsTexture!==null,instancing:Ge,instancingColor:Ge&&U.instanceColor!==null,instancingMorph:Ge&&U.morphTexture!==null,outputColorSpace:ne===null?n.outputColorSpace:ne.isXRRenderTarget===!0?ne.texture.colorSpace:Js,alphaToCoverage:!!y.alphaToCoverage,map:Le,matcap:Xt,envMap:it,envMapMode:it&&L.mapping,envMapCubeUVHeight:Q,aoMap:ht,lightMap:Mt,bumpMap:Ye,normalMap:Pt,displacementMap:A,emissiveMap:Ft,normalMapObjectSpace:Pt&&y.normalMapType===cw,normalMapTangentSpace:Pt&&y.normalMapType===a_,metalnessMap:at,roughnessMap:Et,anisotropy:be,anisotropyMap:W,clearcoat:b,clearcoatMap:ve,clearcoatNormalMap:re,clearcoatRoughnessMap:De,dispersion:v,iridescence:N,iridescenceMap:Pe,iridescenceThicknessMap:K,sheen:$,sheenColorMap:ee,sheenRoughnessMap:ye,specularMap:xe,specularColorMap:fe,specularIntensityMap:Ze,transmission:Y,transmissionMap:R,thicknessMap:se,gradientMap:te,opaque:y.transparent===!1&&y.blending===Zs&&y.alphaToCoverage===!1,alphaMap:me,alphaTest:J,alphaHash:G,combine:y.combine,mapUv:Le&&g(y.map.channel),aoMapUv:ht&&g(y.aoMap.channel),lightMapUv:Mt&&g(y.lightMap.channel),bumpMapUv:Ye&&g(y.bumpMap.channel),normalMapUv:Pt&&g(y.normalMap.channel),displacementMapUv:A&&g(y.displacementMap.channel),emissiveMapUv:Ft&&g(y.emissiveMap.channel),metalnessMapUv:at&&g(y.metalnessMap.channel),roughnessMapUv:Et&&g(y.roughnessMap.channel),anisotropyMapUv:W&&g(y.anisotropyMap.channel),clearcoatMapUv:ve&&g(y.clearcoatMap.channel),clearcoatNormalMapUv:re&&g(y.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:De&&g(y.clearcoatRoughnessMap.channel),iridescenceMapUv:Pe&&g(y.iridescenceMap.channel),iridescenceThicknessMapUv:K&&g(y.iridescenceThicknessMap.channel),sheenColorMapUv:ee&&g(y.sheenColorMap.channel),sheenRoughnessMapUv:ye&&g(y.sheenRoughnessMap.channel),specularMapUv:xe&&g(y.specularMap.channel),specularColorMapUv:fe&&g(y.specularColorMap.channel),specularIntensityMapUv:Ze&&g(y.specularIntensityMap.channel),transmissionMapUv:R&&g(y.transmissionMap.channel),thicknessMapUv:se&&g(y.thicknessMap.channel),alphaMapUv:me&&g(y.alphaMap.channel),vertexTangents:!!j.attributes.tangent&&(Pt||be),vertexColors:y.vertexColors,vertexAlphas:y.vertexColors===!0&&!!j.attributes.color&&j.attributes.color.itemSize===4,pointsUvs:U.isPoints===!0&&!!j.attributes.uv&&(Le||me),fog:!!V,useFog:y.fog===!0,fogExp2:!!V&&V.isFogExp2,flatShading:y.wireframe===!1&&(y.flatShading===!0||j.attributes.normal===void 0&&Pt===!1&&(y.isMeshLambertMaterial||y.isMeshPhongMaterial||y.isMeshStandardMaterial||y.isMeshPhysicalMaterial)),sizeAttenuation:y.sizeAttenuation===!0,logarithmicDepthBuffer:d,reversedDepthBuffer:oe,skinning:U.isSkinnedMesh===!0,morphTargets:j.morphAttributes.position!==void 0,morphNormals:j.morphAttributes.normal!==void 0,morphColors:j.morphAttributes.color!==void 0,morphTargetsCount:ge,morphTextureStride:he,numDirLights:E.directional.length,numPointLights:E.point.length,numSpotLights:E.spot.length,numSpotLightMaps:E.spotLightMap.length,numRectAreaLights:E.rectArea.length,numHemiLights:E.hemi.length,numDirLightShadows:E.directionalShadowMap.length,numPointLightShadows:E.pointShadowMap.length,numSpotLightShadows:E.spotShadowMap.length,numSpotLightShadowsWithMaps:E.numSpotLightShadowsWithMaps,numLightProbes:E.numLightProbes,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:y.dithering,shadowMapEnabled:n.shadowMap.enabled&&q.length>0,shadowMapType:n.shadowMap.type,toneMapping:ke,decodeVideoTexture:Le&&y.map.isVideoTexture===!0&&nt.getTransfer(y.map.colorSpace)===ft,decodeVideoTextureEmissive:Ft&&y.emissiveMap.isVideoTexture===!0&&nt.getTransfer(y.emissiveMap.colorSpace)===ft,premultipliedAlpha:y.premultipliedAlpha,doubleSided:y.side===Gi,flipSided:y.side===Cn,useDepthPacking:y.depthPacking>=0,depthPacking:y.depthPacking||0,index0AttributeName:y.index0AttributeName,extensionClipCullDistance:_e&&y.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(_e&&y.extensions.multiDraw===!0||Ie)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:y.customProgramCacheKey()};return St.vertexUv1s=c.has(1),St.vertexUv2s=c.has(2),St.vertexUv3s=c.has(3),c.clear(),St}function m(y){let E=[];if(y.shaderID?E.push(y.shaderID):(E.push(y.customVertexShaderID),E.push(y.customFragmentShaderID)),y.defines!==void 0)for(let q in y.defines)E.push(q),E.push(y.defines[q]);return y.isRawShaderMaterial===!1&&(p(E,y),M(E,y),E.push(n.outputColorSpace)),E.push(y.customProgramCacheKey),E.join()}function p(y,E){y.push(E.precision),y.push(E.outputColorSpace),y.push(E.envMapMode),y.push(E.envMapCubeUVHeight),y.push(E.mapUv),y.push(E.alphaMapUv),y.push(E.lightMapUv),y.push(E.aoMapUv),y.push(E.bumpMapUv),y.push(E.normalMapUv),y.push(E.displacementMapUv),y.push(E.emissiveMapUv),y.push(E.metalnessMapUv),y.push(E.roughnessMapUv),y.push(E.anisotropyMapUv),y.push(E.clearcoatMapUv),y.push(E.clearcoatNormalMapUv),y.push(E.clearcoatRoughnessMapUv),y.push(E.iridescenceMapUv),y.push(E.iridescenceThicknessMapUv),y.push(E.sheenColorMapUv),y.push(E.sheenRoughnessMapUv),y.push(E.specularMapUv),y.push(E.specularColorMapUv),y.push(E.specularIntensityMapUv),y.push(E.transmissionMapUv),y.push(E.thicknessMapUv),y.push(E.combine),y.push(E.fogExp2),y.push(E.sizeAttenuation),y.push(E.morphTargetsCount),y.push(E.morphAttributeCount),y.push(E.numDirLights),y.push(E.numPointLights),y.push(E.numSpotLights),y.push(E.numSpotLightMaps),y.push(E.numHemiLights),y.push(E.numRectAreaLights),y.push(E.numDirLightShadows),y.push(E.numPointLightShadows),y.push(E.numSpotLightShadows),y.push(E.numSpotLightShadowsWithMaps),y.push(E.numLightProbes),y.push(E.shadowMapType),y.push(E.toneMapping),y.push(E.numClippingPlanes),y.push(E.numClipIntersection),y.push(E.depthPacking)}function M(y,E){o.disableAll(),E.instancing&&o.enable(0),E.instancingColor&&o.enable(1),E.instancingMorph&&o.enable(2),E.matcap&&o.enable(3),E.envMap&&o.enable(4),E.normalMapObjectSpace&&o.enable(5),E.normalMapTangentSpace&&o.enable(6),E.clearcoat&&o.enable(7),E.iridescence&&o.enable(8),E.alphaTest&&o.enable(9),E.vertexColors&&o.enable(10),E.vertexAlphas&&o.enable(11),E.vertexUv1s&&o.enable(12),E.vertexUv2s&&o.enable(13),E.vertexUv3s&&o.enable(14),E.vertexTangents&&o.enable(15),E.anisotropy&&o.enable(16),E.alphaHash&&o.enable(17),E.batching&&o.enable(18),E.dispersion&&o.enable(19),E.batchingColor&&o.enable(20),E.gradientMap&&o.enable(21),y.push(o.mask),o.disableAll(),E.fog&&o.enable(0),E.useFog&&o.enable(1),E.flatShading&&o.enable(2),E.logarithmicDepthBuffer&&o.enable(3),E.reversedDepthBuffer&&o.enable(4),E.skinning&&o.enable(5),E.morphTargets&&o.enable(6),E.morphNormals&&o.enable(7),E.morphColors&&o.enable(8),E.premultipliedAlpha&&o.enable(9),E.shadowMapEnabled&&o.enable(10),E.doubleSided&&o.enable(11),E.flipSided&&o.enable(12),E.useDepthPacking&&o.enable(13),E.dithering&&o.enable(14),E.transmission&&o.enable(15),E.sheen&&o.enable(16),E.opaque&&o.enable(17),E.pointsUvs&&o.enable(18),E.decodeVideoTexture&&o.enable(19),E.decodeVideoTextureEmissive&&o.enable(20),E.alphaToCoverage&&o.enable(21),y.push(o.mask)}function w(y){let E=h[y.type],q;if(E){let C=Yi[E];q=Mw.clone(C.uniforms)}else q=y.uniforms;return q}function S(y,E){let q=u.get(E);return q!==void 0?++q.usedTimes:(q=new h2(n,E,y,r),l.push(q),u.set(E,q)),q}function T(y){if(--y.usedTimes===0){let E=l.indexOf(y);l[E]=l[l.length-1],l.pop(),u.delete(y.cacheKey),y.destroy()}}function D(y){a.remove(y)}function I(){a.dispose()}return{getParameters:_,getProgramCacheKey:m,getUniforms:w,acquireProgram:S,releaseProgram:T,releaseShaderCache:D,programs:l,dispose:I}}function g2(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function v2(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function Vw(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function Hw(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(f){let h=0;return f.isInstancedMesh&&(h+=2),f.isSkinnedMesh&&(h+=1),h}function a(f,h,g,_,m,p){let M=n[e];return M===void 0?(M={id:f.id,object:f,geometry:h,material:g,materialVariant:o(f),groupOrder:_,renderOrder:f.renderOrder,z:m,group:p},n[e]=M):(M.id=f.id,M.object=f,M.geometry=h,M.material=g,M.materialVariant=o(f),M.groupOrder=_,M.renderOrder=f.renderOrder,M.z=m,M.group=p),e++,M}function c(f,h,g,_,m,p){let M=a(f,h,g,_,m,p);g.transmission>0?i.push(M):g.transparent===!0?r.push(M):t.push(M)}function l(f,h,g,_,m,p){let M=a(f,h,g,_,m,p);g.transmission>0?i.unshift(M):g.transparent===!0?r.unshift(M):t.unshift(M)}function u(f,h){t.length>1&&t.sort(f||v2),i.length>1&&i.sort(h||Vw),r.length>1&&r.sort(h||Vw)}function d(){for(let f=e,h=n.length;f<h;f++){let g=n[f];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:d,sort:u}}function y2(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new Hw,n.set(i,[o])):r>=s.length?(o=new Hw,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function _2(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new P,color:new We};break;case"SpotLight":t={position:new P,direction:new P,color:new We,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new P,color:new We,distance:0,decay:0};break;case"HemisphereLight":t={direction:new P,skyColor:new We,groundColor:new We};break;case"RectAreaLight":t={color:new We,position:new P,halfWidth:new P,halfHeight:new P};break}return n[e.id]=t,t}}}function x2(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Ne};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Ne};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Ne,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var M2=0;function b2(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function E2(n){let e=new _2,t=x2(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new P);let r=new P,s=new Rt,o=new Rt;function a(l){let u=0,d=0,f=0;for(let E=0;E<9;E++)i.probe[E].set(0,0,0);let h=0,g=0,_=0,m=0,p=0,M=0,w=0,S=0,T=0,D=0,I=0;l.sort(b2);for(let E=0,q=l.length;E<q;E++){let C=l[E],U=C.color,V=C.intensity,j=C.distance,B=null;if(C.shadow&&C.shadow.map&&(C.shadow.map.texture.format===so?B=C.shadow.map.texture:B=C.shadow.map.depthTexture||C.shadow.map.texture),C.isAmbientLight)u+=U.r*V,d+=U.g*V,f+=U.b*V;else if(C.isLightProbe){for(let H=0;H<9;H++)i.probe[H].addScaledVector(C.sh.coefficients[H],V);I++}else if(C.isDirectionalLight){let H=e.get(C);if(H.color.copy(C.color).multiplyScalar(C.intensity),C.castShadow){let L=C.shadow,Q=t.get(C);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,i.directionalShadow[h]=Q,i.directionalShadowMap[h]=B,i.directionalShadowMatrix[h]=C.shadow.matrix,M++}i.directional[h]=H,h++}else if(C.isSpotLight){let H=e.get(C);H.position.setFromMatrixPosition(C.matrixWorld),H.color.copy(U).multiplyScalar(V),H.distance=j,H.coneCos=Math.cos(C.angle),H.penumbraCos=Math.cos(C.angle*(1-C.penumbra)),H.decay=C.decay,i.spot[_]=H;let L=C.shadow;if(C.map&&(i.spotLightMap[T]=C.map,T++,L.updateMatrices(C),C.castShadow&&D++),i.spotLightMatrix[_]=L.matrix,C.castShadow){let Q=t.get(C);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,i.spotShadow[_]=Q,i.spotShadowMap[_]=B,S++}_++}else if(C.isRectAreaLight){let H=e.get(C);H.color.copy(U).multiplyScalar(V),H.halfWidth.set(C.width*.5,0,0),H.halfHeight.set(0,C.height*.5,0),i.rectArea[m]=H,m++}else if(C.isPointLight){let H=e.get(C);if(H.color.copy(C.color).multiplyScalar(C.intensity),H.distance=C.distance,H.decay=C.decay,C.castShadow){let L=C.shadow,Q=t.get(C);Q.shadowIntensity=L.intensity,Q.shadowBias=L.bias,Q.shadowNormalBias=L.normalBias,Q.shadowRadius=L.radius,Q.shadowMapSize=L.mapSize,Q.shadowCameraNear=L.camera.near,Q.shadowCameraFar=L.camera.far,i.pointShadow[g]=Q,i.pointShadowMap[g]=B,i.pointShadowMatrix[g]=C.shadow.matrix,w++}i.point[g]=H,g++}else if(C.isHemisphereLight){let H=e.get(C);H.skyColor.copy(C.color).multiplyScalar(V),H.groundColor.copy(C.groundColor).multiplyScalar(V),i.hemi[p]=H,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=ce.LTC_FLOAT_1,i.rectAreaLTC2=ce.LTC_FLOAT_2):(i.rectAreaLTC1=ce.LTC_HALF_1,i.rectAreaLTC2=ce.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let y=i.hash;(y.directionalLength!==h||y.pointLength!==g||y.spotLength!==_||y.rectAreaLength!==m||y.hemiLength!==p||y.numDirectionalShadows!==M||y.numPointShadows!==w||y.numSpotShadows!==S||y.numSpotMaps!==T||y.numLightProbes!==I)&&(i.directional.length=h,i.spot.length=_,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=M,i.directionalShadowMap.length=M,i.pointShadow.length=w,i.pointShadowMap.length=w,i.spotShadow.length=S,i.spotShadowMap.length=S,i.directionalShadowMatrix.length=M,i.pointShadowMatrix.length=w,i.spotLightMatrix.length=S+T-D,i.spotLightMap.length=T,i.numSpotLightShadowsWithMaps=D,i.numLightProbes=I,y.directionalLength=h,y.pointLength=g,y.spotLength=_,y.rectAreaLength=m,y.hemiLength=p,y.numDirectionalShadows=M,y.numPointShadows=w,y.numSpotShadows=S,y.numSpotMaps=T,y.numLightProbes=I,i.version=M2++)}function c(l,u){let d=0,f=0,h=0,g=0,_=0,m=u.matrixWorldInverse;for(let p=0,M=l.length;p<M;p++){let w=l[p];if(w.isDirectionalLight){let S=i.directional[d];S.direction.setFromMatrixPosition(w.matrixWorld),r.setFromMatrixPosition(w.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),d++}else if(w.isSpotLight){let S=i.spot[h];S.position.setFromMatrixPosition(w.matrixWorld),S.position.applyMatrix4(m),S.direction.setFromMatrixPosition(w.matrixWorld),r.setFromMatrixPosition(w.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),h++}else if(w.isRectAreaLight){let S=i.rectArea[g];S.position.setFromMatrixPosition(w.matrixWorld),S.position.applyMatrix4(m),o.identity(),s.copy(w.matrixWorld),s.premultiply(m),o.extractRotation(s),S.halfWidth.set(w.width*.5,0,0),S.halfHeight.set(0,w.height*.5,0),S.halfWidth.applyMatrix4(o),S.halfHeight.applyMatrix4(o),g++}else if(w.isPointLight){let S=i.point[f];S.position.setFromMatrixPosition(w.matrixWorld),S.position.applyMatrix4(m),f++}else if(w.isHemisphereLight){let S=i.hemi[_];S.direction.setFromMatrixPosition(w.matrixWorld),S.direction.transformDirection(m),_++}}}return{setup:a,setupView:c,state:i}}function zw(n){let e=new E2(n),t=[],i=[];function r(u){l.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function c(u){e.setupView(t,u)}let l={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:l,setupLights:a,setupLightsView:c,pushLight:s,pushShadow:o}}function S2(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new zw(n),e.set(r,[a])):s>=o.length?(a=new zw(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var w2=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,C2=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,D2=[new P(1,0,0),new P(-1,0,0),new P(0,1,0),new P(0,-1,0),new P(0,0,1),new P(0,0,-1)],T2=[new P(0,-1,0),new P(0,-1,0),new P(0,0,1),new P(0,0,-1),new P(0,-1,0),new P(0,-1,0)],Gw=new Rt,Bl=new P,x_=new P;function A2(n,e,t){let i=new xl,r=new Ne,s=new Ne,o=new Nt,a=new nh,c=new ih,l={},u=t.maxTextureSize,d={[Sr]:Cn,[Cn]:Sr,[Gi]:Gi},f=new jn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new Ne},radius:{value:4}},vertexShader:w2,fragmentShader:C2}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let g=new vn;g.setAttribute("position",new An(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let _=new wn(g,f),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=Rl;let p=this.type;this.render=function(D,I,y){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||D.length===0)return;this.type===BS&&(Ae("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=Rl);let E=n.getRenderTarget(),q=n.getActiveCubeFace(),C=n.getActiveMipmapLevel(),U=n.state;U.setBlending(ji),U.buffers.depth.getReversed()===!0?U.buffers.color.setClear(0,0,0,0):U.buffers.color.setClear(1,1,1,1),U.buffers.depth.setTest(!0),U.setScissorTest(!1);let V=p!==this.type;V&&I.traverse(function(j){j.material&&(Array.isArray(j.material)?j.material.forEach(B=>B.needsUpdate=!0):j.material.needsUpdate=!0)});for(let j=0,B=D.length;j<B;j++){let H=D[j],L=H.shadow;if(L===void 0){Ae("WebGLShadowMap:",H,"has no shadow.");continue}if(L.autoUpdate===!1&&L.needsUpdate===!1)continue;r.copy(L.mapSize);let Q=L.getFrameExtents();r.multiply(Q),s.copy(L.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/Q.x),r.x=s.x*Q.x,L.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/Q.y),r.y=s.y*Q.y,L.mapSize.y=s.y));let Z=n.state.buffers.depth.getReversed();if(L.camera._reversedDepth=Z,L.map===null||V===!0){if(L.map!==null&&(L.map.depthTexture!==null&&(L.map.depthTexture.dispose(),L.map.depthTexture=null),L.map.dispose()),this.type===Ia){if(H.isPointLight){Ae("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}L.map=new Gn(r.x,r.y,{format:so,type:Wi,minFilter:cn,magFilter:cn,generateMipmaps:!1}),L.map.texture.name=H.name+".shadowMap",L.map.depthTexture=new ts(r.x,r.y,Mi),L.map.depthTexture.name=H.name+".shadowMapDepth",L.map.depthTexture.format=Vi,L.map.depthTexture.compareFunction=null,L.map.depthTexture.minFilter=en,L.map.depthTexture.magFilter=en}else H.isPointLight?(L.map=new ap(r.x),L.map.depthTexture=new eh(r.x,xi)):(L.map=new Gn(r.x,r.y),L.map.depthTexture=new ts(r.x,r.y,xi)),L.map.depthTexture.name=H.name+".shadowMap",L.map.depthTexture.format=Vi,this.type===Rl?(L.map.depthTexture.compareFunction=Z?ip:np,L.map.depthTexture.minFilter=cn,L.map.depthTexture.magFilter=cn):(L.map.depthTexture.compareFunction=null,L.map.depthTexture.minFilter=en,L.map.depthTexture.magFilter=en);L.camera.updateProjectionMatrix()}let de=L.map.isWebGLCubeRenderTarget?6:1;for(let ge=0;ge<de;ge++){if(L.map.isWebGLCubeRenderTarget)n.setRenderTarget(L.map,ge),n.clear();else{ge===0&&(n.setRenderTarget(L.map),n.clear());let he=L.getViewport(ge);o.set(s.x*he.x,s.y*he.y,s.x*he.z,s.y*he.w),U.viewport(o)}if(H.isPointLight){let he=L.camera,$e=L.matrix,Ct=H.distance||he.far;Ct!==he.far&&(he.far=Ct,he.updateProjectionMatrix()),Bl.setFromMatrixPosition(H.matrixWorld),he.position.copy(Bl),x_.copy(he.position),x_.add(D2[ge]),he.up.copy(T2[ge]),he.lookAt(x_),he.updateMatrixWorld(),$e.makeTranslation(-Bl.x,-Bl.y,-Bl.z),Gw.multiplyMatrices(he.projectionMatrix,he.matrixWorldInverse),L._frustum.setFromProjectionMatrix(Gw,he.coordinateSystem,he.reversedDepth)}else L.updateMatrices(H);i=L.getFrustum(),S(I,y,L.camera,H,this.type)}L.isPointLightShadow!==!0&&this.type===Ia&&M(L,y),L.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(E,q,C)};function M(D,I){let y=e.update(_);f.defines.VSM_SAMPLES!==D.blurSamples&&(f.defines.VSM_SAMPLES=D.blurSamples,h.defines.VSM_SAMPLES=D.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),D.mapPass===null&&(D.mapPass=new Gn(r.x,r.y,{format:so,type:Wi})),f.uniforms.shadow_pass.value=D.map.depthTexture,f.uniforms.resolution.value=D.mapSize,f.uniforms.radius.value=D.radius,n.setRenderTarget(D.mapPass),n.clear(),n.renderBufferDirect(I,null,y,f,_,null),h.uniforms.shadow_pass.value=D.mapPass.texture,h.uniforms.resolution.value=D.mapSize,h.uniforms.radius.value=D.radius,n.setRenderTarget(D.map),n.clear(),n.renderBufferDirect(I,null,y,h,_,null)}function w(D,I,y,E){let q=null,C=y.isPointLight===!0?D.customDistanceMaterial:D.customDepthMaterial;if(C!==void 0)q=C;else if(q=y.isPointLight===!0?c:a,n.localClippingEnabled&&I.clipShadows===!0&&Array.isArray(I.clippingPlanes)&&I.clippingPlanes.length!==0||I.displacementMap&&I.displacementScale!==0||I.alphaMap&&I.alphaTest>0||I.map&&I.alphaTest>0||I.alphaToCoverage===!0){let U=q.uuid,V=I.uuid,j=l[U];j===void 0&&(j={},l[U]=j);let B=j[V];B===void 0&&(B=q.clone(),j[V]=B,I.addEventListener("dispose",T)),q=B}if(q.visible=I.visible,q.wireframe=I.wireframe,E===Ia?q.side=I.shadowSide!==null?I.shadowSide:I.side:q.side=I.shadowSide!==null?I.shadowSide:d[I.side],q.alphaMap=I.alphaMap,q.alphaTest=I.alphaToCoverage===!0?.5:I.alphaTest,q.map=I.map,q.clipShadows=I.clipShadows,q.clippingPlanes=I.clippingPlanes,q.clipIntersection=I.clipIntersection,q.displacementMap=I.displacementMap,q.displacementScale=I.displacementScale,q.displacementBias=I.displacementBias,q.wireframeLinewidth=I.wireframeLinewidth,q.linewidth=I.linewidth,y.isPointLight===!0&&q.isMeshDistanceMaterial===!0){let U=n.properties.get(q);U.light=y}return q}function S(D,I,y,E,q){if(D.visible===!1)return;if(D.layers.test(I.layers)&&(D.isMesh||D.isLine||D.isPoints)&&(D.castShadow||D.receiveShadow&&q===Ia)&&(!D.frustumCulled||i.intersectsObject(D))){D.modelViewMatrix.multiplyMatrices(y.matrixWorldInverse,D.matrixWorld);let V=e.update(D),j=D.material;if(Array.isArray(j)){let B=V.groups;for(let H=0,L=B.length;H<L;H++){let Q=B[H],Z=j[Q.materialIndex];if(Z&&Z.visible){let de=w(D,Z,E,q);D.onBeforeShadow(n,D,I,y,V,de,Q),n.renderBufferDirect(y,null,V,de,D,Q),D.onAfterShadow(n,D,I,y,V,de,Q)}}}else if(j.visible){let B=w(D,j,E,q);D.onBeforeShadow(n,D,I,y,V,B,null),n.renderBufferDirect(y,null,V,B,D,null),D.onAfterShadow(n,D,I,y,V,B,null)}}let U=D.children;for(let V=0,j=U.length;V<j;V++)S(U[V],I,y,E,q)}function T(D){D.target.removeEventListener("dispose",T);for(let y in l){let E=l[y],q=D.target.uuid;q in E&&(E[q].dispose(),delete E[q])}}}function I2(n,e){function t(){let R=!1,se=new Nt,te=null,me=new Nt(0,0,0,0);return{setMask:function(J){te!==J&&!R&&(n.colorMask(J,J,J,J),te=J)},setLocked:function(J){R=J},setClear:function(J,G,_e,ke,St){St===!0&&(J*=ke,G*=ke,_e*=ke),se.set(J,G,_e,ke),me.equals(se)===!1&&(n.clearColor(J,G,_e,ke),me.copy(se))},reset:function(){R=!1,te=null,me.set(-1,0,0,0)}}}function i(){let R=!1,se=!1,te=null,me=null,J=null;return{setReversed:function(G){if(se!==G){let _e=e.get("EXT_clip_control");G?_e.clipControlEXT(_e.LOWER_LEFT_EXT,_e.ZERO_TO_ONE_EXT):_e.clipControlEXT(_e.LOWER_LEFT_EXT,_e.NEGATIVE_ONE_TO_ONE_EXT),se=G;let ke=J;J=null,this.setClear(ke)}},getReversed:function(){return se},setTest:function(G){G?ne(n.DEPTH_TEST):oe(n.DEPTH_TEST)},setMask:function(G){te!==G&&!R&&(n.depthMask(G),te=G)},setFunc:function(G){if(se&&(G=yw[G]),me!==G){switch(G){case Lf:n.depthFunc(n.NEVER);break;case Ff:n.depthFunc(n.ALWAYS);break;case kf:n.depthFunc(n.LESS);break;case Ks:n.depthFunc(n.LEQUAL);break;case Uf:n.depthFunc(n.EQUAL);break;case Bf:n.depthFunc(n.GEQUAL);break;case Vf:n.depthFunc(n.GREATER);break;case Hf:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}me=G}},setLocked:function(G){R=G},setClear:function(G){J!==G&&(J=G,se&&(G=1-G),n.clearDepth(G))},reset:function(){R=!1,te=null,me=null,J=null,se=!1}}}function r(){let R=!1,se=null,te=null,me=null,J=null,G=null,_e=null,ke=null,St=null;return{setTest:function(pt){R||(pt?ne(n.STENCIL_TEST):oe(n.STENCIL_TEST))},setMask:function(pt){se!==pt&&!R&&(n.stencilMask(pt),se=pt)},setFunc:function(pt,Zi,Ki){(te!==pt||me!==Zi||J!==Ki)&&(n.stencilFunc(pt,Zi,Ki),te=pt,me=Zi,J=Ki)},setOp:function(pt,Zi,Ki){(G!==pt||_e!==Zi||ke!==Ki)&&(n.stencilOp(pt,Zi,Ki),G=pt,_e=Zi,ke=Ki)},setLocked:function(pt){R=pt},setClear:function(pt){St!==pt&&(n.clearStencil(pt),St=pt)},reset:function(){R=!1,se=null,te=null,me=null,J=null,G=null,_e=null,ke=null,St=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},f=new WeakMap,h=[],g=null,_=!1,m=null,p=null,M=null,w=null,S=null,T=null,D=null,I=new We(0,0,0),y=0,E=!1,q=null,C=null,U=null,V=null,j=null,B=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),H=!1,L=0,Q=n.getParameter(n.VERSION);Q.indexOf("WebGL")!==-1?(L=parseFloat(/^WebGL (\d)/.exec(Q)[1]),H=L>=1):Q.indexOf("OpenGL ES")!==-1&&(L=parseFloat(/^OpenGL ES (\d)/.exec(Q)[1]),H=L>=2);let Z=null,de={},ge=n.getParameter(n.SCISSOR_BOX),he=n.getParameter(n.VIEWPORT),$e=new Nt().fromArray(ge),Ct=new Nt().fromArray(he);function wt(R,se,te,me){let J=new Uint8Array(4),G=n.createTexture();n.bindTexture(R,G),n.texParameteri(R,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(R,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let _e=0;_e<te;_e++)R===n.TEXTURE_3D||R===n.TEXTURE_2D_ARRAY?n.texImage3D(se,0,n.RGBA,1,1,me,0,n.RGBA,n.UNSIGNED_BYTE,J):n.texImage2D(se+_e,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,J);return G}let X={};X[n.TEXTURE_2D]=wt(n.TEXTURE_2D,n.TEXTURE_2D,1),X[n.TEXTURE_CUBE_MAP]=wt(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),X[n.TEXTURE_2D_ARRAY]=wt(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),X[n.TEXTURE_3D]=wt(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ne(n.DEPTH_TEST),o.setFunc(Ks),Ye(!1),Pt(zy),ne(n.CULL_FACE),ht(ji);function ne(R){u[R]!==!0&&(n.enable(R),u[R]=!0)}function oe(R){u[R]!==!1&&(n.disable(R),u[R]=!1)}function Ge(R,se){return d[R]!==se?(n.bindFramebuffer(R,se),d[R]=se,R===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=se),R===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=se),!0):!1}function Ie(R,se){let te=h,me=!1;if(R){te=f.get(se),te===void 0&&(te=[],f.set(se,te));let J=R.textures;if(te.length!==J.length||te[0]!==n.COLOR_ATTACHMENT0){for(let G=0,_e=J.length;G<_e;G++)te[G]=n.COLOR_ATTACHMENT0+G;te.length=J.length,me=!0}}else te[0]!==n.BACK&&(te[0]=n.BACK,me=!0);me&&n.drawBuffers(te)}function Le(R){return g!==R?(n.useProgram(R),g=R,!0):!1}let Xt={[Jr]:n.FUNC_ADD,[HS]:n.FUNC_SUBTRACT,[zS]:n.FUNC_REVERSE_SUBTRACT};Xt[GS]=n.MIN,Xt[jS]=n.MAX;let it={[WS]:n.ZERO,[$S]:n.ONE,[qS]:n.SRC_COLOR,[Pf]:n.SRC_ALPHA,[QS]:n.SRC_ALPHA_SATURATE,[KS]:n.DST_COLOR,[YS]:n.DST_ALPHA,[XS]:n.ONE_MINUS_SRC_COLOR,[Of]:n.ONE_MINUS_SRC_ALPHA,[JS]:n.ONE_MINUS_DST_COLOR,[ZS]:n.ONE_MINUS_DST_ALPHA,[ew]:n.CONSTANT_COLOR,[tw]:n.ONE_MINUS_CONSTANT_COLOR,[nw]:n.CONSTANT_ALPHA,[iw]:n.ONE_MINUS_CONSTANT_ALPHA};function ht(R,se,te,me,J,G,_e,ke,St,pt){if(R===ji){_===!0&&(oe(n.BLEND),_=!1);return}if(_===!1&&(ne(n.BLEND),_=!0),R!==VS){if(R!==m||pt!==E){if((p!==Jr||S!==Jr)&&(n.blendEquation(n.FUNC_ADD),p=Jr,S=Jr),pt)switch(R){case Zs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Gy:n.blendFunc(n.ONE,n.ONE);break;case jy:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Wy:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Re("WebGLState: Invalid blending: ",R);break}else switch(R){case Zs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Gy:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case jy:Re("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Wy:Re("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Re("WebGLState: Invalid blending: ",R);break}M=null,w=null,T=null,D=null,I.set(0,0,0),y=0,m=R,E=pt}return}J=J||se,G=G||te,_e=_e||me,(se!==p||J!==S)&&(n.blendEquationSeparate(Xt[se],Xt[J]),p=se,S=J),(te!==M||me!==w||G!==T||_e!==D)&&(n.blendFuncSeparate(it[te],it[me],it[G],it[_e]),M=te,w=me,T=G,D=_e),(ke.equals(I)===!1||St!==y)&&(n.blendColor(ke.r,ke.g,ke.b,St),I.copy(ke),y=St),m=R,E=!1}function Mt(R,se){R.side===Gi?oe(n.CULL_FACE):ne(n.CULL_FACE);let te=R.side===Cn;se&&(te=!te),Ye(te),R.blending===Zs&&R.transparent===!1?ht(ji):ht(R.blending,R.blendEquation,R.blendSrc,R.blendDst,R.blendEquationAlpha,R.blendSrcAlpha,R.blendDstAlpha,R.blendColor,R.blendAlpha,R.premultipliedAlpha),o.setFunc(R.depthFunc),o.setTest(R.depthTest),o.setMask(R.depthWrite),s.setMask(R.colorWrite);let me=R.stencilWrite;a.setTest(me),me&&(a.setMask(R.stencilWriteMask),a.setFunc(R.stencilFunc,R.stencilRef,R.stencilFuncMask),a.setOp(R.stencilFail,R.stencilZFail,R.stencilZPass)),Ft(R.polygonOffset,R.polygonOffsetFactor,R.polygonOffsetUnits),R.alphaToCoverage===!0?ne(n.SAMPLE_ALPHA_TO_COVERAGE):oe(n.SAMPLE_ALPHA_TO_COVERAGE)}function Ye(R){q!==R&&(R?n.frontFace(n.CW):n.frontFace(n.CCW),q=R)}function Pt(R){R!==kS?(ne(n.CULL_FACE),R!==C&&(R===zy?n.cullFace(n.BACK):R===US?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):oe(n.CULL_FACE),C=R}function A(R){R!==U&&(H&&n.lineWidth(R),U=R)}function Ft(R,se,te){R?(ne(n.POLYGON_OFFSET_FILL),(V!==se||j!==te)&&(V=se,j=te,o.getReversed()&&(se=-se),n.polygonOffset(se,te))):oe(n.POLYGON_OFFSET_FILL)}function at(R){R?ne(n.SCISSOR_TEST):oe(n.SCISSOR_TEST)}function Et(R){R===void 0&&(R=n.TEXTURE0+B-1),Z!==R&&(n.activeTexture(R),Z=R)}function be(R,se,te){te===void 0&&(Z===null?te=n.TEXTURE0+B-1:te=Z);let me=de[te];me===void 0&&(me={type:void 0,texture:void 0},de[te]=me),(me.type!==R||me.texture!==se)&&(Z!==te&&(n.activeTexture(te),Z=te),n.bindTexture(R,se||X[R]),me.type=R,me.texture=se)}function b(){let R=de[Z];R!==void 0&&R.type!==void 0&&(n.bindTexture(R.type,null),R.type=void 0,R.texture=void 0)}function v(){try{n.compressedTexImage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function N(){try{n.compressedTexImage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function $(){try{n.texSubImage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function Y(){try{n.texSubImage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function W(){try{n.compressedTexSubImage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function ve(){try{n.compressedTexSubImage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function re(){try{n.texStorage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function De(){try{n.texStorage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function Pe(){try{n.texImage2D(...arguments)}catch(R){Re("WebGLState:",R)}}function K(){try{n.texImage3D(...arguments)}catch(R){Re("WebGLState:",R)}}function ee(R){$e.equals(R)===!1&&(n.scissor(R.x,R.y,R.z,R.w),$e.copy(R))}function ye(R){Ct.equals(R)===!1&&(n.viewport(R.x,R.y,R.z,R.w),Ct.copy(R))}function xe(R,se){let te=l.get(se);te===void 0&&(te=new WeakMap,l.set(se,te));let me=te.get(R);me===void 0&&(me=n.getUniformBlockIndex(se,R.name),te.set(R,me))}function fe(R,se){let me=l.get(se).get(R);c.get(se)!==me&&(n.uniformBlockBinding(se,me,R.__bindingPointIndex),c.set(se,me))}function Ze(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},Z=null,de={},d={},f=new WeakMap,h=[],g=null,_=!1,m=null,p=null,M=null,w=null,S=null,T=null,D=null,I=new We(0,0,0),y=0,E=!1,q=null,C=null,U=null,V=null,j=null,$e.set(0,0,n.canvas.width,n.canvas.height),Ct.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ne,disable:oe,bindFramebuffer:Ge,drawBuffers:Ie,useProgram:Le,setBlending:ht,setMaterial:Mt,setFlipSided:Ye,setCullFace:Pt,setLineWidth:A,setPolygonOffset:Ft,setScissorTest:at,activeTexture:Et,bindTexture:be,unbindTexture:b,compressedTexImage2D:v,compressedTexImage3D:N,texImage2D:Pe,texImage3D:K,updateUBOMapping:xe,uniformBlockBinding:fe,texStorage2D:re,texStorage3D:De,texSubImage2D:$,texSubImage3D:Y,compressedTexSubImage2D:W,compressedTexSubImage3D:ve,scissor:ee,viewport:ye,reset:Ze}}function R2(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new Ne,u=new WeakMap,d,f=new WeakMap,h=!1;try{h=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(b,v){return h?new OffscreenCanvas(b,v):dl("canvas")}function _(b,v,N){let $=1,Y=be(b);if((Y.width>N||Y.height>N)&&($=N/Math.max(Y.width,Y.height)),$<1)if(typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&b instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&b instanceof ImageBitmap||typeof VideoFrame<"u"&&b instanceof VideoFrame){let W=Math.floor($*Y.width),ve=Math.floor($*Y.height);d===void 0&&(d=g(W,ve));let re=v?g(W,ve):d;return re.width=W,re.height=ve,re.getContext("2d").drawImage(b,0,0,W,ve),Ae("WebGLRenderer: Texture has been resized from ("+Y.width+"x"+Y.height+") to ("+W+"x"+ve+")."),re}else return"data"in b&&Ae("WebGLRenderer: Image in DataTexture is too big ("+Y.width+"x"+Y.height+")."),b;return b}function m(b){return b.generateMipmaps}function p(b){n.generateMipmap(b)}function M(b){return b.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:b.isWebGL3DRenderTarget?n.TEXTURE_3D:b.isWebGLArrayRenderTarget||b.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function w(b,v,N,$,Y=!1){if(b!==null){if(n[b]!==void 0)return n[b];Ae("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+b+"'")}let W=v;if(v===n.RED&&(N===n.FLOAT&&(W=n.R32F),N===n.HALF_FLOAT&&(W=n.R16F),N===n.UNSIGNED_BYTE&&(W=n.R8)),v===n.RED_INTEGER&&(N===n.UNSIGNED_BYTE&&(W=n.R8UI),N===n.UNSIGNED_SHORT&&(W=n.R16UI),N===n.UNSIGNED_INT&&(W=n.R32UI),N===n.BYTE&&(W=n.R8I),N===n.SHORT&&(W=n.R16I),N===n.INT&&(W=n.R32I)),v===n.RG&&(N===n.FLOAT&&(W=n.RG32F),N===n.HALF_FLOAT&&(W=n.RG16F),N===n.UNSIGNED_BYTE&&(W=n.RG8)),v===n.RG_INTEGER&&(N===n.UNSIGNED_BYTE&&(W=n.RG8UI),N===n.UNSIGNED_SHORT&&(W=n.RG16UI),N===n.UNSIGNED_INT&&(W=n.RG32UI),N===n.BYTE&&(W=n.RG8I),N===n.SHORT&&(W=n.RG16I),N===n.INT&&(W=n.RG32I)),v===n.RGB_INTEGER&&(N===n.UNSIGNED_BYTE&&(W=n.RGB8UI),N===n.UNSIGNED_SHORT&&(W=n.RGB16UI),N===n.UNSIGNED_INT&&(W=n.RGB32UI),N===n.BYTE&&(W=n.RGB8I),N===n.SHORT&&(W=n.RGB16I),N===n.INT&&(W=n.RGB32I)),v===n.RGBA_INTEGER&&(N===n.UNSIGNED_BYTE&&(W=n.RGBA8UI),N===n.UNSIGNED_SHORT&&(W=n.RGBA16UI),N===n.UNSIGNED_INT&&(W=n.RGBA32UI),N===n.BYTE&&(W=n.RGBA8I),N===n.SHORT&&(W=n.RGBA16I),N===n.INT&&(W=n.RGBA32I)),v===n.RGB&&(N===n.UNSIGNED_INT_5_9_9_9_REV&&(W=n.RGB9_E5),N===n.UNSIGNED_INT_10F_11F_11F_REV&&(W=n.R11F_G11F_B10F)),v===n.RGBA){let ve=Y?ll:nt.getTransfer($);N===n.FLOAT&&(W=n.RGBA32F),N===n.HALF_FLOAT&&(W=n.RGBA16F),N===n.UNSIGNED_BYTE&&(W=ve===ft?n.SRGB8_ALPHA8:n.RGBA8),N===n.UNSIGNED_SHORT_4_4_4_4&&(W=n.RGBA4),N===n.UNSIGNED_SHORT_5_5_5_1&&(W=n.RGB5_A1)}return(W===n.R16F||W===n.R32F||W===n.RG16F||W===n.RG32F||W===n.RGBA16F||W===n.RGBA32F)&&e.get("EXT_color_buffer_float"),W}function S(b,v){let N;return b?v===null||v===xi||v===Na?N=n.DEPTH24_STENCIL8:v===Mi?N=n.DEPTH32F_STENCIL8:v===Ra&&(N=n.DEPTH24_STENCIL8,Ae("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===xi||v===Na?N=n.DEPTH_COMPONENT24:v===Mi?N=n.DEPTH_COMPONENT32F:v===Ra&&(N=n.DEPTH_COMPONENT16),N}function T(b,v){return m(b)===!0||b.isFramebufferTexture&&b.minFilter!==en&&b.minFilter!==cn?Math.log2(Math.max(v.width,v.height))+1:b.mipmaps!==void 0&&b.mipmaps.length>0?b.mipmaps.length:b.isCompressedTexture&&Array.isArray(b.image)?v.mipmaps.length:1}function D(b){let v=b.target;v.removeEventListener("dispose",D),y(v),v.isVideoTexture&&u.delete(v)}function I(b){let v=b.target;v.removeEventListener("dispose",I),q(v)}function y(b){let v=i.get(b);if(v.__webglInit===void 0)return;let N=b.source,$=f.get(N);if($){let Y=$[v.__cacheKey];Y.usedTimes--,Y.usedTimes===0&&E(b),Object.keys($).length===0&&f.delete(N)}i.remove(b)}function E(b){let v=i.get(b);n.deleteTexture(v.__webglTexture);let N=b.source,$=f.get(N);delete $[v.__cacheKey],o.memory.textures--}function q(b){let v=i.get(b);if(b.depthTexture&&(b.depthTexture.dispose(),i.remove(b.depthTexture)),b.isWebGLCubeRenderTarget)for(let $=0;$<6;$++){if(Array.isArray(v.__webglFramebuffer[$]))for(let Y=0;Y<v.__webglFramebuffer[$].length;Y++)n.deleteFramebuffer(v.__webglFramebuffer[$][Y]);else n.deleteFramebuffer(v.__webglFramebuffer[$]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[$])}else{if(Array.isArray(v.__webglFramebuffer))for(let $=0;$<v.__webglFramebuffer.length;$++)n.deleteFramebuffer(v.__webglFramebuffer[$]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let $=0;$<v.__webglColorRenderbuffer.length;$++)v.__webglColorRenderbuffer[$]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[$]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let N=b.textures;for(let $=0,Y=N.length;$<Y;$++){let W=i.get(N[$]);W.__webglTexture&&(n.deleteTexture(W.__webglTexture),o.memory.textures--),i.remove(N[$])}i.remove(b)}let C=0;function U(){C=0}function V(){let b=C;return b>=r.maxTextures&&Ae("WebGLTextures: Trying to use "+b+" texture units while this GPU supports only "+r.maxTextures),C+=1,b}function j(b){let v=[];return v.push(b.wrapS),v.push(b.wrapT),v.push(b.wrapR||0),v.push(b.magFilter),v.push(b.minFilter),v.push(b.anisotropy),v.push(b.internalFormat),v.push(b.format),v.push(b.type),v.push(b.generateMipmaps),v.push(b.premultiplyAlpha),v.push(b.flipY),v.push(b.unpackAlignment),v.push(b.colorSpace),v.join()}function B(b,v){let N=i.get(b);if(b.isVideoTexture&&at(b),b.isRenderTargetTexture===!1&&b.isExternalTexture!==!0&&b.version>0&&N.__version!==b.version){let $=b.image;if($===null)Ae("WebGLRenderer: Texture marked for update but no image data found.");else if($.complete===!1)Ae("WebGLRenderer: Texture marked for update but image is incomplete");else{X(N,b,v);return}}else b.isExternalTexture&&(N.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,N.__webglTexture,n.TEXTURE0+v)}function H(b,v){let N=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&N.__version!==b.version){X(N,b,v);return}else b.isExternalTexture&&(N.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,N.__webglTexture,n.TEXTURE0+v)}function L(b,v){let N=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&N.__version!==b.version){X(N,b,v);return}t.bindTexture(n.TEXTURE_3D,N.__webglTexture,n.TEXTURE0+v)}function Q(b,v){let N=i.get(b);if(b.isCubeDepthTexture!==!0&&b.version>0&&N.__version!==b.version){ne(N,b,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,N.__webglTexture,n.TEXTURE0+v)}let Z={[zf]:n.REPEAT,[Ui]:n.CLAMP_TO_EDGE,[Gf]:n.MIRRORED_REPEAT},de={[en]:n.NEAREST,[ow]:n.NEAREST_MIPMAP_NEAREST,[Pl]:n.NEAREST_MIPMAP_LINEAR,[cn]:n.LINEAR,[vh]:n.LINEAR_MIPMAP_NEAREST,[cs]:n.LINEAR_MIPMAP_LINEAR},ge={[lw]:n.NEVER,[pw]:n.ALWAYS,[uw]:n.LESS,[np]:n.LEQUAL,[dw]:n.EQUAL,[ip]:n.GEQUAL,[fw]:n.GREATER,[hw]:n.NOTEQUAL};function he(b,v){if(v.type===Mi&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===cn||v.magFilter===vh||v.magFilter===Pl||v.magFilter===cs||v.minFilter===cn||v.minFilter===vh||v.minFilter===Pl||v.minFilter===cs)&&Ae("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(b,n.TEXTURE_WRAP_S,Z[v.wrapS]),n.texParameteri(b,n.TEXTURE_WRAP_T,Z[v.wrapT]),(b===n.TEXTURE_3D||b===n.TEXTURE_2D_ARRAY)&&n.texParameteri(b,n.TEXTURE_WRAP_R,Z[v.wrapR]),n.texParameteri(b,n.TEXTURE_MAG_FILTER,de[v.magFilter]),n.texParameteri(b,n.TEXTURE_MIN_FILTER,de[v.minFilter]),v.compareFunction&&(n.texParameteri(b,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(b,n.TEXTURE_COMPARE_FUNC,ge[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===en||v.minFilter!==Pl&&v.minFilter!==cs||v.type===Mi&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let N=e.get("EXT_texture_filter_anisotropic");n.texParameterf(b,N.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function $e(b,v){let N=!1;b.__webglInit===void 0&&(b.__webglInit=!0,v.addEventListener("dispose",D));let $=v.source,Y=f.get($);Y===void 0&&(Y={},f.set($,Y));let W=j(v);if(W!==b.__cacheKey){Y[W]===void 0&&(Y[W]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,N=!0),Y[W].usedTimes++;let ve=Y[b.__cacheKey];ve!==void 0&&(Y[b.__cacheKey].usedTimes--,ve.usedTimes===0&&E(v)),b.__cacheKey=W,b.__webglTexture=Y[W].texture}return N}function Ct(b,v,N){return Math.floor(Math.floor(b/N)/v)}function wt(b,v,N,$){let W=b.updateRanges;if(W.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,N,$,v.data);else{W.sort((K,ee)=>K.start-ee.start);let ve=0;for(let K=1;K<W.length;K++){let ee=W[ve],ye=W[K],xe=ee.start+ee.count,fe=Ct(ye.start,v.width,4),Ze=Ct(ee.start,v.width,4);ye.start<=xe+1&&fe===Ze&&Ct(ye.start+ye.count-1,v.width,4)===fe?ee.count=Math.max(ee.count,ye.start+ye.count-ee.start):(++ve,W[ve]=ye)}W.length=ve+1;let re=n.getParameter(n.UNPACK_ROW_LENGTH),De=n.getParameter(n.UNPACK_SKIP_PIXELS),Pe=n.getParameter(n.UNPACK_SKIP_ROWS);n.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let K=0,ee=W.length;K<ee;K++){let ye=W[K],xe=Math.floor(ye.start/4),fe=Math.ceil(ye.count/4),Ze=xe%v.width,R=Math.floor(xe/v.width),se=fe,te=1;n.pixelStorei(n.UNPACK_SKIP_PIXELS,Ze),n.pixelStorei(n.UNPACK_SKIP_ROWS,R),t.texSubImage2D(n.TEXTURE_2D,0,Ze,R,se,te,N,$,v.data)}b.clearUpdateRanges(),n.pixelStorei(n.UNPACK_ROW_LENGTH,re),n.pixelStorei(n.UNPACK_SKIP_PIXELS,De),n.pixelStorei(n.UNPACK_SKIP_ROWS,Pe)}}function X(b,v,N){let $=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&($=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&($=n.TEXTURE_3D);let Y=$e(b,v),W=v.source;t.bindTexture($,b.__webglTexture,n.TEXTURE0+N);let ve=i.get(W);if(W.version!==ve.__version||Y===!0){t.activeTexture(n.TEXTURE0+N);let re=nt.getPrimaries(nt.workingColorSpace),De=v.colorSpace===wr?null:nt.getPrimaries(v.colorSpace),Pe=v.colorSpace===wr||re===De?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Pe);let K=_(v.image,!1,r.maxTextureSize);K=Et(v,K);let ee=s.convert(v.format,v.colorSpace),ye=s.convert(v.type),xe=w(v.internalFormat,ee,ye,v.colorSpace,v.isVideoTexture);he($,v);let fe,Ze=v.mipmaps,R=v.isVideoTexture!==!0,se=ve.__version===void 0||Y===!0,te=W.dataReady,me=T(v,K);if(v.isDepthTexture)xe=S(v.format===ls,v.type),se&&(R?t.texStorage2D(n.TEXTURE_2D,1,xe,K.width,K.height):t.texImage2D(n.TEXTURE_2D,0,xe,K.width,K.height,0,ee,ye,null));else if(v.isDataTexture)if(Ze.length>0){R&&se&&t.texStorage2D(n.TEXTURE_2D,me,xe,Ze[0].width,Ze[0].height);for(let J=0,G=Ze.length;J<G;J++)fe=Ze[J],R?te&&t.texSubImage2D(n.TEXTURE_2D,J,0,0,fe.width,fe.height,ee,ye,fe.data):t.texImage2D(n.TEXTURE_2D,J,xe,fe.width,fe.height,0,ee,ye,fe.data);v.generateMipmaps=!1}else R?(se&&t.texStorage2D(n.TEXTURE_2D,me,xe,K.width,K.height),te&&wt(v,K,ee,ye)):t.texImage2D(n.TEXTURE_2D,0,xe,K.width,K.height,0,ee,ye,K.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){R&&se&&t.texStorage3D(n.TEXTURE_2D_ARRAY,me,xe,Ze[0].width,Ze[0].height,K.depth);for(let J=0,G=Ze.length;J<G;J++)if(fe=Ze[J],v.format!==ii)if(ee!==null)if(R){if(te)if(v.layerUpdates.size>0){let _e=p_(fe.width,fe.height,v.format,v.type);for(let ke of v.layerUpdates){let St=fe.data.subarray(ke*_e/fe.data.BYTES_PER_ELEMENT,(ke+1)*_e/fe.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,J,0,0,ke,fe.width,fe.height,1,ee,St)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,J,0,0,0,fe.width,fe.height,K.depth,ee,fe.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,J,xe,fe.width,fe.height,K.depth,0,fe.data,0,0);else Ae("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else R?te&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,J,0,0,0,fe.width,fe.height,K.depth,ee,ye,fe.data):t.texImage3D(n.TEXTURE_2D_ARRAY,J,xe,fe.width,fe.height,K.depth,0,ee,ye,fe.data)}else{R&&se&&t.texStorage2D(n.TEXTURE_2D,me,xe,Ze[0].width,Ze[0].height);for(let J=0,G=Ze.length;J<G;J++)fe=Ze[J],v.format!==ii?ee!==null?R?te&&t.compressedTexSubImage2D(n.TEXTURE_2D,J,0,0,fe.width,fe.height,ee,fe.data):t.compressedTexImage2D(n.TEXTURE_2D,J,xe,fe.width,fe.height,0,fe.data):Ae("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):R?te&&t.texSubImage2D(n.TEXTURE_2D,J,0,0,fe.width,fe.height,ee,ye,fe.data):t.texImage2D(n.TEXTURE_2D,J,xe,fe.width,fe.height,0,ee,ye,fe.data)}else if(v.isDataArrayTexture)if(R){if(se&&t.texStorage3D(n.TEXTURE_2D_ARRAY,me,xe,K.width,K.height,K.depth),te)if(v.layerUpdates.size>0){let J=p_(K.width,K.height,v.format,v.type);for(let G of v.layerUpdates){let _e=K.data.subarray(G*J/K.data.BYTES_PER_ELEMENT,(G+1)*J/K.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,G,K.width,K.height,1,ee,ye,_e)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,K.width,K.height,K.depth,ee,ye,K.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,xe,K.width,K.height,K.depth,0,ee,ye,K.data);else if(v.isData3DTexture)R?(se&&t.texStorage3D(n.TEXTURE_3D,me,xe,K.width,K.height,K.depth),te&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,K.width,K.height,K.depth,ee,ye,K.data)):t.texImage3D(n.TEXTURE_3D,0,xe,K.width,K.height,K.depth,0,ee,ye,K.data);else if(v.isFramebufferTexture){if(se)if(R)t.texStorage2D(n.TEXTURE_2D,me,xe,K.width,K.height);else{let J=K.width,G=K.height;for(let _e=0;_e<me;_e++)t.texImage2D(n.TEXTURE_2D,_e,xe,J,G,0,ee,ye,null),J>>=1,G>>=1}}else if(Ze.length>0){if(R&&se){let J=be(Ze[0]);t.texStorage2D(n.TEXTURE_2D,me,xe,J.width,J.height)}for(let J=0,G=Ze.length;J<G;J++)fe=Ze[J],R?te&&t.texSubImage2D(n.TEXTURE_2D,J,0,0,ee,ye,fe):t.texImage2D(n.TEXTURE_2D,J,xe,ee,ye,fe);v.generateMipmaps=!1}else if(R){if(se){let J=be(K);t.texStorage2D(n.TEXTURE_2D,me,xe,J.width,J.height)}te&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ee,ye,K)}else t.texImage2D(n.TEXTURE_2D,0,xe,ee,ye,K);m(v)&&p($),ve.__version=W.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function ne(b,v,N){if(v.image.length!==6)return;let $=$e(b,v),Y=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,b.__webglTexture,n.TEXTURE0+N);let W=i.get(Y);if(Y.version!==W.__version||$===!0){t.activeTexture(n.TEXTURE0+N);let ve=nt.getPrimaries(nt.workingColorSpace),re=v.colorSpace===wr?null:nt.getPrimaries(v.colorSpace),De=v.colorSpace===wr||ve===re?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,De);let Pe=v.isCompressedTexture||v.image[0].isCompressedTexture,K=v.image[0]&&v.image[0].isDataTexture,ee=[];for(let G=0;G<6;G++)!Pe&&!K?ee[G]=_(v.image[G],!0,r.maxCubemapSize):ee[G]=K?v.image[G].image:v.image[G],ee[G]=Et(v,ee[G]);let ye=ee[0],xe=s.convert(v.format,v.colorSpace),fe=s.convert(v.type),Ze=w(v.internalFormat,xe,fe,v.colorSpace),R=v.isVideoTexture!==!0,se=W.__version===void 0||$===!0,te=Y.dataReady,me=T(v,ye);he(n.TEXTURE_CUBE_MAP,v);let J;if(Pe){R&&se&&t.texStorage2D(n.TEXTURE_CUBE_MAP,me,Ze,ye.width,ye.height);for(let G=0;G<6;G++){J=ee[G].mipmaps;for(let _e=0;_e<J.length;_e++){let ke=J[_e];v.format!==ii?xe!==null?R?te&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,0,0,ke.width,ke.height,xe,ke.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,Ze,ke.width,ke.height,0,ke.data):Ae("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,0,0,ke.width,ke.height,xe,fe,ke.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e,Ze,ke.width,ke.height,0,xe,fe,ke.data)}}}else{if(J=v.mipmaps,R&&se){J.length>0&&me++;let G=be(ee[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,me,Ze,G.width,G.height)}for(let G=0;G<6;G++)if(K){R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,0,0,ee[G].width,ee[G].height,xe,fe,ee[G].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,Ze,ee[G].width,ee[G].height,0,xe,fe,ee[G].data);for(let _e=0;_e<J.length;_e++){let St=J[_e].image[G].image;R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,0,0,St.width,St.height,xe,fe,St.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,Ze,St.width,St.height,0,xe,fe,St.data)}}else{R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,0,0,xe,fe,ee[G]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0,Ze,xe,fe,ee[G]);for(let _e=0;_e<J.length;_e++){let ke=J[_e];R?te&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,0,0,xe,fe,ke.image[G]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+G,_e+1,Ze,xe,fe,ke.image[G])}}}m(v)&&p(n.TEXTURE_CUBE_MAP),W.__version=Y.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function oe(b,v,N,$,Y,W){let ve=s.convert(N.format,N.colorSpace),re=s.convert(N.type),De=w(N.internalFormat,ve,re,N.colorSpace),Pe=i.get(v),K=i.get(N);if(K.__renderTarget=v,!Pe.__hasExternalTextures){let ee=Math.max(1,v.width>>W),ye=Math.max(1,v.height>>W);Y===n.TEXTURE_3D||Y===n.TEXTURE_2D_ARRAY?t.texImage3D(Y,W,De,ee,ye,v.depth,0,ve,re,null):t.texImage2D(Y,W,De,ee,ye,0,ve,re,null)}t.bindFramebuffer(n.FRAMEBUFFER,b),Ft(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,$,Y,K.__webglTexture,0,A(v)):(Y===n.TEXTURE_2D||Y>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&Y<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,$,Y,K.__webglTexture,W),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ge(b,v,N){if(n.bindRenderbuffer(n.RENDERBUFFER,b),v.depthBuffer){let $=v.depthTexture,Y=$&&$.isDepthTexture?$.type:null,W=S(v.stencilBuffer,Y),ve=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Ft(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,A(v),W,v.width,v.height):N?n.renderbufferStorageMultisample(n.RENDERBUFFER,A(v),W,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,W,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,ve,n.RENDERBUFFER,b)}else{let $=v.textures;for(let Y=0;Y<$.length;Y++){let W=$[Y],ve=s.convert(W.format,W.colorSpace),re=s.convert(W.type),De=w(W.internalFormat,ve,re,W.colorSpace);Ft(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,A(v),De,v.width,v.height):N?n.renderbufferStorageMultisample(n.RENDERBUFFER,A(v),De,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,De,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ie(b,v,N){let $=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,b),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let Y=i.get(v.depthTexture);if(Y.__renderTarget=v,(!Y.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),$){if(Y.__webglInit===void 0&&(Y.__webglInit=!0,v.depthTexture.addEventListener("dispose",D)),Y.__webglTexture===void 0){Y.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,Y.__webglTexture),he(n.TEXTURE_CUBE_MAP,v.depthTexture);let Pe=s.convert(v.depthTexture.format),K=s.convert(v.depthTexture.type),ee;v.depthTexture.format===Vi?ee=n.DEPTH_COMPONENT24:v.depthTexture.format===ls&&(ee=n.DEPTH24_STENCIL8);for(let ye=0;ye<6;ye++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+ye,0,ee,v.width,v.height,0,Pe,K,null)}}else B(v.depthTexture,0);let W=Y.__webglTexture,ve=A(v),re=$?n.TEXTURE_CUBE_MAP_POSITIVE_X+N:n.TEXTURE_2D,De=v.depthTexture.format===ls?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===Vi)Ft(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,De,re,W,0,ve):n.framebufferTexture2D(n.FRAMEBUFFER,De,re,W,0);else if(v.depthTexture.format===ls)Ft(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,De,re,W,0,ve):n.framebufferTexture2D(n.FRAMEBUFFER,De,re,W,0);else throw new Error("Unknown depthTexture format")}function Le(b){let v=i.get(b),N=b.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==b.depthTexture){let $=b.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),$){let Y=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,$.removeEventListener("dispose",Y)};$.addEventListener("dispose",Y),v.__depthDisposeCallback=Y}v.__boundDepthTexture=$}if(b.depthTexture&&!v.__autoAllocateDepthBuffer)if(N)for(let $=0;$<6;$++)Ie(v.__webglFramebuffer[$],b,$);else{let $=b.texture.mipmaps;$&&$.length>0?Ie(v.__webglFramebuffer[0],b,0):Ie(v.__webglFramebuffer,b,0)}else if(N){v.__webglDepthbuffer=[];for(let $=0;$<6;$++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[$]),v.__webglDepthbuffer[$]===void 0)v.__webglDepthbuffer[$]=n.createRenderbuffer(),Ge(v.__webglDepthbuffer[$],b,!1);else{let Y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,W=v.__webglDepthbuffer[$];n.bindRenderbuffer(n.RENDERBUFFER,W),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,W)}}else{let $=b.texture.mipmaps;if($&&$.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Ge(v.__webglDepthbuffer,b,!1);else{let Y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,W=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,W),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,W)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Xt(b,v,N){let $=i.get(b);v!==void 0&&oe($.__webglFramebuffer,b,b.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),N!==void 0&&Le(b)}function it(b){let v=b.texture,N=i.get(b),$=i.get(v);b.addEventListener("dispose",I);let Y=b.textures,W=b.isWebGLCubeRenderTarget===!0,ve=Y.length>1;if(ve||($.__webglTexture===void 0&&($.__webglTexture=n.createTexture()),$.__version=v.version,o.memory.textures++),W){N.__webglFramebuffer=[];for(let re=0;re<6;re++)if(v.mipmaps&&v.mipmaps.length>0){N.__webglFramebuffer[re]=[];for(let De=0;De<v.mipmaps.length;De++)N.__webglFramebuffer[re][De]=n.createFramebuffer()}else N.__webglFramebuffer[re]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){N.__webglFramebuffer=[];for(let re=0;re<v.mipmaps.length;re++)N.__webglFramebuffer[re]=n.createFramebuffer()}else N.__webglFramebuffer=n.createFramebuffer();if(ve)for(let re=0,De=Y.length;re<De;re++){let Pe=i.get(Y[re]);Pe.__webglTexture===void 0&&(Pe.__webglTexture=n.createTexture(),o.memory.textures++)}if(b.samples>0&&Ft(b)===!1){N.__webglMultisampledFramebuffer=n.createFramebuffer(),N.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,N.__webglMultisampledFramebuffer);for(let re=0;re<Y.length;re++){let De=Y[re];N.__webglColorRenderbuffer[re]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,N.__webglColorRenderbuffer[re]);let Pe=s.convert(De.format,De.colorSpace),K=s.convert(De.type),ee=w(De.internalFormat,Pe,K,De.colorSpace,b.isXRRenderTarget===!0),ye=A(b);n.renderbufferStorageMultisample(n.RENDERBUFFER,ye,ee,b.width,b.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+re,n.RENDERBUFFER,N.__webglColorRenderbuffer[re])}n.bindRenderbuffer(n.RENDERBUFFER,null),b.depthBuffer&&(N.__webglDepthRenderbuffer=n.createRenderbuffer(),Ge(N.__webglDepthRenderbuffer,b,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(W){t.bindTexture(n.TEXTURE_CUBE_MAP,$.__webglTexture),he(n.TEXTURE_CUBE_MAP,v);for(let re=0;re<6;re++)if(v.mipmaps&&v.mipmaps.length>0)for(let De=0;De<v.mipmaps.length;De++)oe(N.__webglFramebuffer[re][De],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+re,De);else oe(N.__webglFramebuffer[re],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+re,0);m(v)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(ve){for(let re=0,De=Y.length;re<De;re++){let Pe=Y[re],K=i.get(Pe),ee=n.TEXTURE_2D;(b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(ee=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(ee,K.__webglTexture),he(ee,Pe),oe(N.__webglFramebuffer,b,Pe,n.COLOR_ATTACHMENT0+re,ee,0),m(Pe)&&p(ee)}t.unbindTexture()}else{let re=n.TEXTURE_2D;if((b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(re=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(re,$.__webglTexture),he(re,v),v.mipmaps&&v.mipmaps.length>0)for(let De=0;De<v.mipmaps.length;De++)oe(N.__webglFramebuffer[De],b,v,n.COLOR_ATTACHMENT0,re,De);else oe(N.__webglFramebuffer,b,v,n.COLOR_ATTACHMENT0,re,0);m(v)&&p(re),t.unbindTexture()}b.depthBuffer&&Le(b)}function ht(b){let v=b.textures;for(let N=0,$=v.length;N<$;N++){let Y=v[N];if(m(Y)){let W=M(b),ve=i.get(Y).__webglTexture;t.bindTexture(W,ve),p(W),t.unbindTexture()}}}let Mt=[],Ye=[];function Pt(b){if(b.samples>0){if(Ft(b)===!1){let v=b.textures,N=b.width,$=b.height,Y=n.COLOR_BUFFER_BIT,W=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ve=i.get(b),re=v.length>1;if(re)for(let Pe=0;Pe<v.length;Pe++)t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Pe,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+Pe,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,ve.__webglMultisampledFramebuffer);let De=b.texture.mipmaps;De&&De.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglFramebuffer);for(let Pe=0;Pe<v.length;Pe++){if(b.resolveDepthBuffer&&(b.depthBuffer&&(Y|=n.DEPTH_BUFFER_BIT),b.stencilBuffer&&b.resolveStencilBuffer&&(Y|=n.STENCIL_BUFFER_BIT)),re){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,ve.__webglColorRenderbuffer[Pe]);let K=i.get(v[Pe]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,K,0)}n.blitFramebuffer(0,0,N,$,0,0,N,$,Y,n.NEAREST),c===!0&&(Mt.length=0,Ye.length=0,Mt.push(n.COLOR_ATTACHMENT0+Pe),b.depthBuffer&&b.resolveDepthBuffer===!1&&(Mt.push(W),Ye.push(W),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,Ye)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Mt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),re)for(let Pe=0;Pe<v.length;Pe++){t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Pe,n.RENDERBUFFER,ve.__webglColorRenderbuffer[Pe]);let K=i.get(v[Pe]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,ve.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+Pe,n.TEXTURE_2D,K,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ve.__webglMultisampledFramebuffer)}else if(b.depthBuffer&&b.resolveDepthBuffer===!1&&c){let v=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function A(b){return Math.min(r.maxSamples,b.samples)}function Ft(b){let v=i.get(b);return b.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function at(b){let v=o.render.frame;u.get(b)!==v&&(u.set(b,v),b.update())}function Et(b,v){let N=b.colorSpace,$=b.format,Y=b.type;return b.isCompressedTexture===!0||b.isVideoTexture===!0||N!==Js&&N!==wr&&(nt.getTransfer(N)===ft?($!==ii||Y!==$n)&&Ae("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Re("WebGLTextures: Unsupported texture color space:",N)),v}function be(b){return typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement?(l.width=b.naturalWidth||b.width,l.height=b.naturalHeight||b.height):typeof VideoFrame<"u"&&b instanceof VideoFrame?(l.width=b.displayWidth,l.height=b.displayHeight):(l.width=b.width,l.height=b.height),l}this.allocateTextureUnit=V,this.resetTextureUnits=U,this.setTexture2D=B,this.setTexture2DArray=H,this.setTexture3D=L,this.setTextureCube=Q,this.rebindTextures=Xt,this.setupRenderTarget=it,this.updateRenderTargetMipmap=ht,this.updateMultisampleRenderTarget=Pt,this.setupDepthRenderbuffer=Le,this.setupFrameBufferTexture=oe,this.useMultisampledRTT=Ft,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function N2(n,e){function t(i,r=wr){let s,o=nt.getTransfer(r);if(i===$n)return n.UNSIGNED_BYTE;if(i===_h)return n.UNSIGNED_SHORT_4_4_4_4;if(i===xh)return n.UNSIGNED_SHORT_5_5_5_1;if(i===n_)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===i_)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===e_)return n.BYTE;if(i===t_)return n.SHORT;if(i===Ra)return n.UNSIGNED_SHORT;if(i===yh)return n.INT;if(i===xi)return n.UNSIGNED_INT;if(i===Mi)return n.FLOAT;if(i===Wi)return n.HALF_FLOAT;if(i===r_)return n.ALPHA;if(i===s_)return n.RGB;if(i===ii)return n.RGBA;if(i===Vi)return n.DEPTH_COMPONENT;if(i===ls)return n.DEPTH_STENCIL;if(i===o_)return n.RED;if(i===Mh)return n.RED_INTEGER;if(i===so)return n.RG;if(i===bh)return n.RG_INTEGER;if(i===Eh)return n.RGBA_INTEGER;if(i===Ol||i===Ll||i===Fl||i===kl)if(o===ft)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Ol)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Ll)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===Fl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===kl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Ol)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Ll)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===Fl)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===kl)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===Sh||i===wh||i===Ch||i===Dh)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===Sh)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===wh)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===Ch)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Dh)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===Th||i===Ah||i===Ih||i===Rh||i===Nh||i===Ph||i===Oh)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===Th||i===Ah)return o===ft?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===Ih)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===Rh)return s.COMPRESSED_R11_EAC;if(i===Nh)return s.COMPRESSED_SIGNED_R11_EAC;if(i===Ph)return s.COMPRESSED_RG11_EAC;if(i===Oh)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===Lh||i===Fh||i===kh||i===Uh||i===Bh||i===Vh||i===Hh||i===zh||i===Gh||i===jh||i===Wh||i===$h||i===qh||i===Xh)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===Lh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Fh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===kh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Uh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Bh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Vh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Hh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===zh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Gh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===jh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Wh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===$h)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===qh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Xh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Yh||i===Zh||i===Kh)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Yh)return o===ft?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Zh)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Kh)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Jh||i===Qh||i===ep||i===tp)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Jh)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Qh)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===ep)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===tp)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Na?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var P2=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,O2=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,T_=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new bl(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new jn({vertexShader:P2,fragmentShader:O2,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new wn(new El(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},A_=class extends Hi{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,d=null,f=null,h=null,g=null,_=typeof XRWebGLBinding<"u",m=new T_,p={},M=t.getContextAttributes(),w=null,S=null,T=[],D=[],I=new Ne,y=null,E=new gn;E.viewport=new Nt;let q=new gn;q.viewport=new Nt;let C=[E,q],U=new ph,V=null,j=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(X){let ne=T[X];return ne===void 0&&(ne=new Ea,T[X]=ne),ne.getTargetRaySpace()},this.getControllerGrip=function(X){let ne=T[X];return ne===void 0&&(ne=new Ea,T[X]=ne),ne.getGripSpace()},this.getHand=function(X){let ne=T[X];return ne===void 0&&(ne=new Ea,T[X]=ne),ne.getHandSpace()};function B(X){let ne=D.indexOf(X.inputSource);if(ne===-1)return;let oe=T[ne];oe!==void 0&&(oe.update(X.inputSource,X.frame,l||o),oe.dispatchEvent({type:X.type,data:X.inputSource}))}function H(){r.removeEventListener("select",B),r.removeEventListener("selectstart",B),r.removeEventListener("selectend",B),r.removeEventListener("squeeze",B),r.removeEventListener("squeezestart",B),r.removeEventListener("squeezeend",B),r.removeEventListener("end",H),r.removeEventListener("inputsourceschange",L);for(let X=0;X<T.length;X++){let ne=D[X];ne!==null&&(D[X]=null,T[X].disconnect(ne))}V=null,j=null,m.reset();for(let X in p)delete p[X];e.setRenderTarget(w),h=null,f=null,d=null,r=null,S=null,wt.stop(),i.isPresenting=!1,e.setPixelRatio(y),e.setSize(I.width,I.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(X){s=X,i.isPresenting===!0&&Ae("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(X){a=X,i.isPresenting===!0&&Ae("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(X){l=X},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d===null&&_&&(d=new XRWebGLBinding(r,t)),d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=async function(X){if(r=X,r!==null){if(w=e.getRenderTarget(),r.addEventListener("select",B),r.addEventListener("selectstart",B),r.addEventListener("selectend",B),r.addEventListener("squeeze",B),r.addEventListener("squeezestart",B),r.addEventListener("squeezeend",B),r.addEventListener("end",H),r.addEventListener("inputsourceschange",L),M.xrCompatible!==!0&&await t.makeXRCompatible(),y=e.getPixelRatio(),e.getSize(I),_&&"createProjectionLayer"in XRWebGLBinding.prototype){let oe=null,Ge=null,Ie=null;M.depth&&(Ie=M.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,oe=M.stencil?ls:Vi,Ge=M.stencil?Na:xi);let Le={colorFormat:t.RGBA8,depthFormat:Ie,scaleFactor:s};d=this.getBinding(),f=d.createProjectionLayer(Le),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),S=new Gn(f.textureWidth,f.textureHeight,{format:ii,type:$n,depthTexture:new ts(f.textureWidth,f.textureHeight,Ge,void 0,void 0,void 0,void 0,void 0,void 0,oe),stencilBuffer:M.stencil,colorSpace:e.outputColorSpace,samples:M.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1,resolveStencilBuffer:f.ignoreDepthValues===!1})}else{let oe={antialias:M.antialias,alpha:!0,depth:M.depth,stencil:M.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,oe),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),S=new Gn(h.framebufferWidth,h.framebufferHeight,{format:ii,type:$n,colorSpace:e.outputColorSpace,stencilBuffer:M.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=await r.requestReferenceSpace(a),wt.setContext(r),wt.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function L(X){for(let ne=0;ne<X.removed.length;ne++){let oe=X.removed[ne],Ge=D.indexOf(oe);Ge>=0&&(D[Ge]=null,T[Ge].disconnect(oe))}for(let ne=0;ne<X.added.length;ne++){let oe=X.added[ne],Ge=D.indexOf(oe);if(Ge===-1){for(let Le=0;Le<T.length;Le++)if(Le>=D.length){D.push(oe),Ge=Le;break}else if(D[Le]===null){D[Le]=oe,Ge=Le;break}if(Ge===-1)break}let Ie=T[Ge];Ie&&Ie.connect(oe)}}let Q=new P,Z=new P;function de(X,ne,oe){Q.setFromMatrixPosition(ne.matrixWorld),Z.setFromMatrixPosition(oe.matrixWorld);let Ge=Q.distanceTo(Z),Ie=ne.projectionMatrix.elements,Le=oe.projectionMatrix.elements,Xt=Ie[14]/(Ie[10]-1),it=Ie[14]/(Ie[10]+1),ht=(Ie[9]+1)/Ie[5],Mt=(Ie[9]-1)/Ie[5],Ye=(Ie[8]-1)/Ie[0],Pt=(Le[8]+1)/Le[0],A=Xt*Ye,Ft=Xt*Pt,at=Ge/(-Ye+Pt),Et=at*-Ye;if(ne.matrixWorld.decompose(X.position,X.quaternion,X.scale),X.translateX(Et),X.translateZ(at),X.matrixWorld.compose(X.position,X.quaternion,X.scale),X.matrixWorldInverse.copy(X.matrixWorld).invert(),Ie[10]===-1)X.projectionMatrix.copy(ne.projectionMatrix),X.projectionMatrixInverse.copy(ne.projectionMatrixInverse);else{let be=Xt+at,b=it+at,v=A-Et,N=Ft+(Ge-Et),$=ht*it/b*be,Y=Mt*it/b*be;X.projectionMatrix.makePerspective(v,N,$,Y,be,b),X.projectionMatrixInverse.copy(X.projectionMatrix).invert()}}function ge(X,ne){ne===null?X.matrixWorld.copy(X.matrix):X.matrixWorld.multiplyMatrices(ne.matrixWorld,X.matrix),X.matrixWorldInverse.copy(X.matrixWorld).invert()}this.updateCamera=function(X){if(r===null)return;let ne=X.near,oe=X.far;m.texture!==null&&(m.depthNear>0&&(ne=m.depthNear),m.depthFar>0&&(oe=m.depthFar)),U.near=q.near=E.near=ne,U.far=q.far=E.far=oe,(V!==U.near||j!==U.far)&&(r.updateRenderState({depthNear:U.near,depthFar:U.far}),V=U.near,j=U.far),U.layers.mask=X.layers.mask|6,E.layers.mask=U.layers.mask&-5,q.layers.mask=U.layers.mask&-3;let Ge=X.parent,Ie=U.cameras;ge(U,Ge);for(let Le=0;Le<Ie.length;Le++)ge(Ie[Le],Ge);Ie.length===2?de(U,E,q):U.projectionMatrix.copy(E.projectionMatrix),he(X,U,Ge)};function he(X,ne,oe){oe===null?X.matrix.copy(ne.matrixWorld):(X.matrix.copy(oe.matrixWorld),X.matrix.invert(),X.matrix.multiply(ne.matrixWorld)),X.matrix.decompose(X.position,X.quaternion,X.scale),X.updateMatrixWorld(!0),X.projectionMatrix.copy(ne.projectionMatrix),X.projectionMatrixInverse.copy(ne.projectionMatrixInverse),X.isPerspectiveCamera&&(X.fov=Ma*2*Math.atan(1/X.projectionMatrix.elements[5]),X.zoom=1)}this.getCamera=function(){return U},this.getFoveation=function(){if(!(f===null&&h===null))return c},this.setFoveation=function(X){c=X,f!==null&&(f.fixedFoveation=X),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=X)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(U)},this.getCameraTexture=function(X){return p[X]};let $e=null;function Ct(X,ne){if(u=ne.getViewerPose(l||o),g=ne,u!==null){let oe=u.views;h!==null&&(e.setRenderTargetFramebuffer(S,h.framebuffer),e.setRenderTarget(S));let Ge=!1;oe.length!==U.cameras.length&&(U.cameras.length=0,Ge=!0);for(let it=0;it<oe.length;it++){let ht=oe[it],Mt=null;if(h!==null)Mt=h.getViewport(ht);else{let Pt=d.getViewSubImage(f,ht);Mt=Pt.viewport,it===0&&(e.setRenderTargetTextures(S,Pt.colorTexture,Pt.depthStencilTexture),e.setRenderTarget(S))}let Ye=C[it];Ye===void 0&&(Ye=new gn,Ye.layers.enable(it),Ye.viewport=new Nt,C[it]=Ye),Ye.matrix.fromArray(ht.transform.matrix),Ye.matrix.decompose(Ye.position,Ye.quaternion,Ye.scale),Ye.projectionMatrix.fromArray(ht.projectionMatrix),Ye.projectionMatrixInverse.copy(Ye.projectionMatrix).invert(),Ye.viewport.set(Mt.x,Mt.y,Mt.width,Mt.height),it===0&&(U.matrix.copy(Ye.matrix),U.matrix.decompose(U.position,U.quaternion,U.scale)),Ge===!0&&U.cameras.push(Ye)}let Ie=r.enabledFeatures;if(Ie&&Ie.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&_){d=i.getBinding();let it=d.getDepthInformation(oe[0]);it&&it.isValid&&it.texture&&m.init(it,r.renderState)}if(Ie&&Ie.includes("camera-access")&&_){e.state.unbindTexture(),d=i.getBinding();for(let it=0;it<oe.length;it++){let ht=oe[it].camera;if(ht){let Mt=p[ht];Mt||(Mt=new bl,p[ht]=Mt);let Ye=d.getCameraImage(ht);Mt.sourceTexture=Ye}}}}for(let oe=0;oe<T.length;oe++){let Ge=D[oe],Ie=T[oe];Ge!==null&&Ie!==void 0&&Ie.update(Ge,ne,l||o)}$e&&$e(X,ne),ne.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:ne}),g=null}let wt=new jw;wt.setAnimationLoop(Ct),this.setAnimationLoop=function(X){$e=X},this.dispose=function(){}}},co=new Qr,L2=new Rt;function F2(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,d_(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,M,w,S){p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),f(m,p),p.isMeshPhysicalMaterial&&h(m,p,S)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),_(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,M,w):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===Cn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===Cn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let M=e.get(p),w=M.envMap,S=M.envMapRotation;w&&(m.envMap.value=w,co.copy(S),co.x*=-1,co.y*=-1,co.z*=-1,w.isCubeTexture&&w.isRenderTargetTexture===!1&&(co.y*=-1,co.z*=-1),m.envMapRotation.value.setFromMatrix4(L2.makeRotationFromEuler(co)),m.flipEnvMap.value=w.isCubeTexture&&w.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,M,w){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*M,m.scale.value=w*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function f(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,M){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===Cn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=M.texture,m.transmissionSamplerSize.value.set(M.width,M.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function _(m,p){let M=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(M.matrixWorld),m.nearDistance.value=M.shadow.camera.near,m.farDistance.value=M.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function k2(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(M,w){let S=w.program;i.uniformBlockBinding(M,S)}function l(M,w){let S=r[M.id];S===void 0&&(g(M),S=u(M),r[M.id]=S,M.addEventListener("dispose",m));let T=w.program;i.updateUBOMapping(M,T);let D=e.render.frame;s[M.id]!==D&&(f(M),s[M.id]=D)}function u(M){let w=d();M.__bindingPointIndex=w;let S=n.createBuffer(),T=M.__size,D=M.usage;return n.bindBuffer(n.UNIFORM_BUFFER,S),n.bufferData(n.UNIFORM_BUFFER,T,D),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,w,S),S}function d(){for(let M=0;M<a;M++)if(o.indexOf(M)===-1)return o.push(M),M;return Re("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function f(M){let w=r[M.id],S=M.uniforms,T=M.__cache;n.bindBuffer(n.UNIFORM_BUFFER,w);for(let D=0,I=S.length;D<I;D++){let y=Array.isArray(S[D])?S[D]:[S[D]];for(let E=0,q=y.length;E<q;E++){let C=y[E];if(h(C,D,E,T)===!0){let U=C.__offset,V=Array.isArray(C.value)?C.value:[C.value],j=0;for(let B=0;B<V.length;B++){let H=V[B],L=_(H);typeof H=="number"||typeof H=="boolean"?(C.__data[0]=H,n.bufferSubData(n.UNIFORM_BUFFER,U+j,C.__data)):H.isMatrix3?(C.__data[0]=H.elements[0],C.__data[1]=H.elements[1],C.__data[2]=H.elements[2],C.__data[3]=0,C.__data[4]=H.elements[3],C.__data[5]=H.elements[4],C.__data[6]=H.elements[5],C.__data[7]=0,C.__data[8]=H.elements[6],C.__data[9]=H.elements[7],C.__data[10]=H.elements[8],C.__data[11]=0):(H.toArray(C.__data,j),j+=L.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,U,C.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(M,w,S,T){let D=M.value,I=w+"_"+S;if(T[I]===void 0)return typeof D=="number"||typeof D=="boolean"?T[I]=D:T[I]=D.clone(),!0;{let y=T[I];if(typeof D=="number"||typeof D=="boolean"){if(y!==D)return T[I]=D,!0}else if(y.equals(D)===!1)return y.copy(D),!0}return!1}function g(M){let w=M.uniforms,S=0,T=16;for(let I=0,y=w.length;I<y;I++){let E=Array.isArray(w[I])?w[I]:[w[I]];for(let q=0,C=E.length;q<C;q++){let U=E[q],V=Array.isArray(U.value)?U.value:[U.value];for(let j=0,B=V.length;j<B;j++){let H=V[j],L=_(H),Q=S%T,Z=Q%L.boundary,de=Q+Z;S+=Z,de!==0&&T-de<L.storage&&(S+=T-de),U.__data=new Float32Array(L.storage/Float32Array.BYTES_PER_ELEMENT),U.__offset=S,S+=L.storage}}}let D=S%T;return D>0&&(S+=T-D),M.__size=S,M.__cache={},this}function _(M){let w={boundary:0,storage:0};return typeof M=="number"||typeof M=="boolean"?(w.boundary=4,w.storage=4):M.isVector2?(w.boundary=8,w.storage=8):M.isVector3||M.isColor?(w.boundary=16,w.storage=12):M.isVector4?(w.boundary=16,w.storage=16):M.isMatrix3?(w.boundary=48,w.storage=48):M.isMatrix4?(w.boundary=64,w.storage=64):M.isTexture?Ae("WebGLRenderer: Texture samplers can not be part of an uniforms group."):Ae("WebGLRenderer: Unsupported uniform value type.",M),w}function m(M){let w=M.target;w.removeEventListener("dispose",m);let S=o.indexOf(w.__bindingPointIndex);o.splice(S,1),n.deleteBuffer(r[w.id]),delete r[w.id],delete s[w.id]}function p(){for(let M in r)n.deleteBuffer(r[M]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var U2=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),Xi=null;function B2(){return Xi===null&&(Xi=new Zf(U2,16,16,so,Wi),Xi.name="DFG_LUT",Xi.minFilter=cn,Xi.magFilter=cn,Xi.wrapS=Ui,Xi.wrapT=Ui,Xi.generateMipmaps=!1,Xi.needsUpdate=!0),Xi}var cp=class{constructor(e={}){let{canvas:t=mw(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reversedDepthBuffer:f=!1,outputBufferType:h=$n}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let _=h,m=new Set([Eh,bh,Mh]),p=new Set([$n,xi,Ra,Na,_h,xh]),M=new Uint32Array(4),w=new Int32Array(4),S=null,T=null,D=[],I=[],y=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=_i,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let E=this,q=!1;this._outputColorSpace=Hn;let C=0,U=0,V=null,j=-1,B=null,H=new Nt,L=new Nt,Q=null,Z=new We(0),de=0,ge=t.width,he=t.height,$e=1,Ct=null,wt=null,X=new Nt(0,0,ge,he),ne=new Nt(0,0,ge,he),oe=!1,Ge=new xl,Ie=!1,Le=!1,Xt=new Rt,it=new P,ht=new Nt,Mt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Ye=!1;function Pt(){return V===null?$e:1}let A=i;function Ft(x,O){return t.getContext(x,O)}try{let x={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"183"}`),t.addEventListener("webglcontextlost",_e,!1),t.addEventListener("webglcontextrestored",ke,!1),t.addEventListener("webglcontextcreationerror",St,!1),A===null){let O="webgl2";if(A=Ft(O,x),A===null)throw Ft(O)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw Re("WebGLRenderer: "+x.message),x}let at,Et,be,b,v,N,$,Y,W,ve,re,De,Pe,K,ee,ye,xe,fe,Ze,R,se,te,me;function J(){at=new qF(A),at.init(),se=new N2(A,at),Et=new BF(A,at,e,se),be=new I2(A,at),Et.reversedDepthBuffer&&f&&be.buffers.depth.setReversed(!0),b=new ZF(A),v=new g2,N=new R2(A,at,be,v,Et,se,b),$=new $F(E),Y=new tO(A),te=new kF(A,Y),W=new XF(A,Y,b,te),ve=new JF(A,W,Y,te,b),fe=new KF(A,Et,N),ee=new VF(v),re=new m2(E,$,at,Et,te,ee),De=new F2(E,v),Pe=new y2,K=new S2(at),xe=new FF(E,$,be,ve,g,c),ye=new A2(E,ve,Et),me=new k2(A,b,Et,be),Ze=new UF(A,at,b),R=new YF(A,at,b),b.programs=re.programs,E.capabilities=Et,E.extensions=at,E.properties=v,E.renderLists=Pe,E.shadowMap=ye,E.state=be,E.info=b}J(),_!==$n&&(y=new ek(_,t.width,t.height,r,s));let G=new A_(E,A);this.xr=G,this.getContext=function(){return A},this.getContextAttributes=function(){return A.getContextAttributes()},this.forceContextLoss=function(){let x=at.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=at.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return $e},this.setPixelRatio=function(x){x!==void 0&&($e=x,this.setSize(ge,he,!1))},this.getSize=function(x){return x.set(ge,he)},this.setSize=function(x,O,z=!0){if(G.isPresenting){Ae("WebGLRenderer: Can't change size while VR device is presenting.");return}ge=x,he=O,t.width=Math.floor(x*$e),t.height=Math.floor(O*$e),z===!0&&(t.style.width=x+"px",t.style.height=O+"px"),y!==null&&y.setSize(t.width,t.height),this.setViewport(0,0,x,O)},this.getDrawingBufferSize=function(x){return x.set(ge*$e,he*$e).floor()},this.setDrawingBufferSize=function(x,O,z){ge=x,he=O,$e=z,t.width=Math.floor(x*z),t.height=Math.floor(O*z),this.setViewport(0,0,x,O)},this.setEffects=function(x){if(_===$n){console.error("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(x){for(let O=0;O<x.length;O++)if(x[O].isOutputPass===!0){console.warn("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}y.setEffects(x||[])},this.getCurrentViewport=function(x){return x.copy(H)},this.getViewport=function(x){return x.copy(X)},this.setViewport=function(x,O,z,k){x.isVector4?X.set(x.x,x.y,x.z,x.w):X.set(x,O,z,k),be.viewport(H.copy(X).multiplyScalar($e).round())},this.getScissor=function(x){return x.copy(ne)},this.setScissor=function(x,O,z,k){x.isVector4?ne.set(x.x,x.y,x.z,x.w):ne.set(x,O,z,k),be.scissor(L.copy(ne).multiplyScalar($e).round())},this.getScissorTest=function(){return oe},this.setScissorTest=function(x){be.setScissorTest(oe=x)},this.setOpaqueSort=function(x){Ct=x},this.setTransparentSort=function(x){wt=x},this.getClearColor=function(x){return x.copy(xe.getClearColor())},this.setClearColor=function(){xe.setClearColor(...arguments)},this.getClearAlpha=function(){return xe.getClearAlpha()},this.setClearAlpha=function(){xe.setClearAlpha(...arguments)},this.clear=function(x=!0,O=!0,z=!0){let k=0;if(x){let F=!1;if(V!==null){let le=V.texture.format;F=m.has(le)}if(F){let le=V.texture.type,pe=p.has(le),ue=xe.getClearColor(),Me=xe.getClearAlpha(),Se=ue.r,He=ue.g,Ke=ue.b;pe?(M[0]=Se,M[1]=He,M[2]=Ke,M[3]=Me,A.clearBufferuiv(A.COLOR,0,M)):(w[0]=Se,w[1]=He,w[2]=Ke,w[3]=Me,A.clearBufferiv(A.COLOR,0,w))}else k|=A.COLOR_BUFFER_BIT}O&&(k|=A.DEPTH_BUFFER_BIT),z&&(k|=A.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),k!==0&&A.clear(k)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",_e,!1),t.removeEventListener("webglcontextrestored",ke,!1),t.removeEventListener("webglcontextcreationerror",St,!1),xe.dispose(),Pe.dispose(),K.dispose(),v.dispose(),$.dispose(),ve.dispose(),te.dispose(),me.dispose(),re.dispose(),G.dispose(),G.removeEventListener("sessionstart",P_),G.removeEventListener("sessionend",O_),ds.stop()};function _e(x){x.preventDefault(),fl("WebGLRenderer: Context Lost."),q=!0}function ke(){fl("WebGLRenderer: Context Restored."),q=!1;let x=b.autoReset,O=ye.enabled,z=ye.autoUpdate,k=ye.needsUpdate,F=ye.type;J(),b.autoReset=x,ye.enabled=O,ye.autoUpdate=z,ye.needsUpdate=k,ye.type=F}function St(x){Re("WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function pt(x){let O=x.target;O.removeEventListener("dispose",pt),Zi(O)}function Zi(x){Ki(x),v.remove(x)}function Ki(x){let O=v.get(x).programs;O!==void 0&&(O.forEach(function(z){re.releaseProgram(z)}),x.isShaderMaterial&&re.releaseShaderCache(x))}this.renderBufferDirect=function(x,O,z,k,F,le){O===null&&(O=Mt);let pe=F.isMesh&&F.matrixWorld.determinant()<0,ue=Qw(x,O,z,k,F);be.setMaterial(k,pe);let Me=z.index,Se=1;if(k.wireframe===!0){if(Me=W.getWireframeAttribute(z),Me===void 0)return;Se=2}let He=z.drawRange,Ke=z.attributes.position,we=He.start*Se,gt=(He.start+He.count)*Se;le!==null&&(we=Math.max(we,le.start*Se),gt=Math.min(gt,(le.start+le.count)*Se)),Me!==null?(we=Math.max(we,0),gt=Math.min(gt,Me.count)):Ke!=null&&(we=Math.max(we,0),gt=Math.min(gt,Ke.count));let Ot=gt-we;if(Ot<0||Ot===1/0)return;te.setup(F,k,ue,z,Me);let At,vt=Ze;if(Me!==null&&(At=Y.get(Me),vt=R,vt.setIndex(At)),F.isMesh)k.wireframe===!0?(be.setLineWidth(k.wireframeLinewidth*Pt()),vt.setMode(A.LINES)):vt.setMode(A.TRIANGLES);else if(F.isLine){let ln=k.linewidth;ln===void 0&&(ln=1),be.setLineWidth(ln*Pt()),F.isLineSegments?vt.setMode(A.LINES):F.isLineLoop?vt.setMode(A.LINE_LOOP):vt.setMode(A.LINE_STRIP)}else F.isPoints?vt.setMode(A.POINTS):F.isSprite&&vt.setMode(A.TRIANGLES);if(F.isBatchedMesh)if(F._multiDrawInstances!==null)hl("WebGLRenderer: renderMultiDrawInstances has been deprecated and will be removed in r184. Append to renderMultiDraw arguments and use indirection."),vt.renderMultiDrawInstances(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount,F._multiDrawInstances);else if(at.get("WEBGL_multi_draw"))vt.renderMultiDraw(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount);else{let ln=F._multiDrawStarts,Ee=F._multiDrawCounts,Rn=F._multiDrawCount,st=Me?Y.get(Me).bytesPerElement:1,ri=v.get(k).currentProgram.getUniforms();for(let bi=0;bi<Rn;bi++)ri.setValue(A,"_gl_DrawID",bi),vt.render(ln[bi]/st,Ee[bi])}else if(F.isInstancedMesh)vt.renderInstances(we,Ot,F.count);else if(z.isInstancedBufferGeometry){let ln=z._maxInstanceCount!==void 0?z._maxInstanceCount:1/0,Ee=Math.min(z.instanceCount,ln);vt.renderInstances(we,Ot,Ee)}else vt.render(we,Ot)};function N_(x,O,z){x.transparent===!0&&x.side===Gi&&x.forceSinglePass===!1?(x.side=Cn,x.needsUpdate=!0,zl(x,O,z),x.side=Sr,x.needsUpdate=!0,zl(x,O,z),x.side=Gi):zl(x,O,z)}this.compile=function(x,O,z=null){z===null&&(z=x),T=K.get(z),T.init(O),I.push(T),z.traverseVisible(function(F){F.isLight&&F.layers.test(O.layers)&&(T.pushLight(F),F.castShadow&&T.pushShadow(F))}),x!==z&&x.traverseVisible(function(F){F.isLight&&F.layers.test(O.layers)&&(T.pushLight(F),F.castShadow&&T.pushShadow(F))}),T.setupLights();let k=new Set;return x.traverse(function(F){if(!(F.isMesh||F.isPoints||F.isLine||F.isSprite))return;let le=F.material;if(le)if(Array.isArray(le))for(let pe=0;pe<le.length;pe++){let ue=le[pe];N_(ue,z,F),k.add(ue)}else N_(le,z,F),k.add(le)}),T=I.pop(),k},this.compileAsync=function(x,O,z=null){let k=this.compile(x,O,z);return new Promise(F=>{function le(){if(k.forEach(function(pe){v.get(pe).currentProgram.isReady()&&k.delete(pe)}),k.size===0){F(x);return}setTimeout(le,10)}at.get("KHR_parallel_shader_compile")!==null?le():setTimeout(le,10)})};let pp=null;function Jw(x){pp&&pp(x)}function P_(){ds.stop()}function O_(){ds.start()}let ds=new jw;ds.setAnimationLoop(Jw),typeof self<"u"&&ds.setContext(self),this.setAnimationLoop=function(x){pp=x,G.setAnimationLoop(x),x===null?ds.stop():ds.start()},G.addEventListener("sessionstart",P_),G.addEventListener("sessionend",O_),this.render=function(x,O){if(O!==void 0&&O.isCamera!==!0){Re("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(q===!0)return;let z=G.enabled===!0&&G.isPresenting===!0,k=y!==null&&(V===null||z)&&y.begin(E,V);if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),O.parent===null&&O.matrixWorldAutoUpdate===!0&&O.updateMatrixWorld(),G.enabled===!0&&G.isPresenting===!0&&(y===null||y.isCompositing()===!1)&&(G.cameraAutoUpdate===!0&&G.updateCamera(O),O=G.getCamera()),x.isScene===!0&&x.onBeforeRender(E,x,O,V),T=K.get(x,I.length),T.init(O),I.push(T),Xt.multiplyMatrices(O.projectionMatrix,O.matrixWorldInverse),Ge.setFromProjectionMatrix(Xt,yi,O.reversedDepth),Le=this.localClippingEnabled,Ie=ee.init(this.clippingPlanes,Le),S=Pe.get(x,D.length),S.init(),D.push(S),G.enabled===!0&&G.isPresenting===!0){let pe=E.xr.getDepthSensingMesh();pe!==null&&mp(pe,O,-1/0,E.sortObjects)}mp(x,O,0,E.sortObjects),S.finish(),E.sortObjects===!0&&S.sort(Ct,wt),Ye=G.enabled===!1||G.isPresenting===!1||G.hasDepthSensing()===!1,Ye&&xe.addToRenderList(S,x),this.info.render.frame++,Ie===!0&&ee.beginShadows();let F=T.state.shadowsArray;if(ye.render(F,x,O),Ie===!0&&ee.endShadows(),this.info.autoReset===!0&&this.info.reset(),(k&&y.hasRenderPass())===!1){let pe=S.opaque,ue=S.transmissive;if(T.setupLights(),O.isArrayCamera){let Me=O.cameras;if(ue.length>0)for(let Se=0,He=Me.length;Se<He;Se++){let Ke=Me[Se];F_(pe,ue,x,Ke)}Ye&&xe.render(x);for(let Se=0,He=Me.length;Se<He;Se++){let Ke=Me[Se];L_(S,x,Ke,Ke.viewport)}}else ue.length>0&&F_(pe,ue,x,O),Ye&&xe.render(x),L_(S,x,O)}V!==null&&U===0&&(N.updateMultisampleRenderTarget(V),N.updateRenderTargetMipmap(V)),k&&y.end(E),x.isScene===!0&&x.onAfterRender(E,x,O),te.resetDefaultState(),j=-1,B=null,I.pop(),I.length>0?(T=I[I.length-1],Ie===!0&&ee.setGlobalState(E.clippingPlanes,T.state.camera)):T=null,D.pop(),D.length>0?S=D[D.length-1]:S=null};function mp(x,O,z,k){if(x.visible===!1)return;if(x.layers.test(O.layers)){if(x.isGroup)z=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(O);else if(x.isLight)T.pushLight(x),x.castShadow&&T.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||Ge.intersectsSprite(x)){k&&ht.setFromMatrixPosition(x.matrixWorld).applyMatrix4(Xt);let pe=ve.update(x),ue=x.material;ue.visible&&S.push(x,pe,ue,z,ht.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||Ge.intersectsObject(x))){let pe=ve.update(x),ue=x.material;if(k&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),ht.copy(x.boundingSphere.center)):(pe.boundingSphere===null&&pe.computeBoundingSphere(),ht.copy(pe.boundingSphere.center)),ht.applyMatrix4(x.matrixWorld).applyMatrix4(Xt)),Array.isArray(ue)){let Me=pe.groups;for(let Se=0,He=Me.length;Se<He;Se++){let Ke=Me[Se],we=ue[Ke.materialIndex];we&&we.visible&&S.push(x,pe,we,z,ht.z,Ke)}}else ue.visible&&S.push(x,pe,ue,z,ht.z,null)}}let le=x.children;for(let pe=0,ue=le.length;pe<ue;pe++)mp(le[pe],O,z,k)}function L_(x,O,z,k){let{opaque:F,transmissive:le,transparent:pe}=x;T.setupLightsView(z),Ie===!0&&ee.setGlobalState(E.clippingPlanes,z),k&&be.viewport(H.copy(k)),F.length>0&&Hl(F,O,z),le.length>0&&Hl(le,O,z),pe.length>0&&Hl(pe,O,z),be.buffers.depth.setTest(!0),be.buffers.depth.setMask(!0),be.buffers.color.setMask(!0),be.setPolygonOffset(!1)}function F_(x,O,z,k){if((z.isScene===!0?z.overrideMaterial:null)!==null)return;if(T.state.transmissionRenderTarget[k.id]===void 0){let we=at.has("EXT_color_buffer_half_float")||at.has("EXT_color_buffer_float");T.state.transmissionRenderTarget[k.id]=new Gn(1,1,{generateMipmaps:!0,type:we?Wi:$n,minFilter:cs,samples:Et.samples,stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:nt.workingColorSpace})}let le=T.state.transmissionRenderTarget[k.id],pe=k.viewport||H;le.setSize(pe.z*E.transmissionResolutionScale,pe.w*E.transmissionResolutionScale);let ue=E.getRenderTarget(),Me=E.getActiveCubeFace(),Se=E.getActiveMipmapLevel();E.setRenderTarget(le),E.getClearColor(Z),de=E.getClearAlpha(),de<1&&E.setClearColor(16777215,.5),E.clear(),Ye&&xe.render(z);let He=E.toneMapping;E.toneMapping=_i;let Ke=k.viewport;if(k.viewport!==void 0&&(k.viewport=void 0),T.setupLightsView(k),Ie===!0&&ee.setGlobalState(E.clippingPlanes,k),Hl(x,z,k),N.updateMultisampleRenderTarget(le),N.updateRenderTargetMipmap(le),at.has("WEBGL_multisampled_render_to_texture")===!1){let we=!1;for(let gt=0,Ot=O.length;gt<Ot;gt++){let At=O[gt],{object:vt,geometry:ln,material:Ee,group:Rn}=At;if(Ee.side===Gi&&vt.layers.test(k.layers)){let st=Ee.side;Ee.side=Cn,Ee.needsUpdate=!0,k_(vt,z,k,ln,Ee,Rn),Ee.side=st,Ee.needsUpdate=!0,we=!0}}we===!0&&(N.updateMultisampleRenderTarget(le),N.updateRenderTargetMipmap(le))}E.setRenderTarget(ue,Me,Se),E.setClearColor(Z,de),Ke!==void 0&&(k.viewport=Ke),E.toneMapping=He}function Hl(x,O,z){let k=O.isScene===!0?O.overrideMaterial:null;for(let F=0,le=x.length;F<le;F++){let pe=x[F],{object:ue,geometry:Me,group:Se}=pe,He=pe.material;He.allowOverride===!0&&k!==null&&(He=k),ue.layers.test(z.layers)&&k_(ue,O,z,Me,He,Se)}}function k_(x,O,z,k,F,le){x.onBeforeRender(E,O,z,k,F,le),x.modelViewMatrix.multiplyMatrices(z.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),F.onBeforeRender(E,O,z,k,x,le),F.transparent===!0&&F.side===Gi&&F.forceSinglePass===!1?(F.side=Cn,F.needsUpdate=!0,E.renderBufferDirect(z,O,k,F,x,le),F.side=Sr,F.needsUpdate=!0,E.renderBufferDirect(z,O,k,F,x,le),F.side=Gi):E.renderBufferDirect(z,O,k,F,x,le),x.onAfterRender(E,O,z,k,F,le)}function zl(x,O,z){O.isScene!==!0&&(O=Mt);let k=v.get(x),F=T.state.lights,le=T.state.shadowsArray,pe=F.state.version,ue=re.getParameters(x,F.state,le,O,z),Me=re.getProgramCacheKey(ue),Se=k.programs;k.environment=x.isMeshStandardMaterial||x.isMeshLambertMaterial||x.isMeshPhongMaterial?O.environment:null,k.fog=O.fog;let He=x.isMeshStandardMaterial||x.isMeshLambertMaterial&&!x.envMap||x.isMeshPhongMaterial&&!x.envMap;k.envMap=$.get(x.envMap||k.environment,He),k.envMapRotation=k.environment!==null&&x.envMap===null?O.environmentRotation:x.envMapRotation,Se===void 0&&(x.addEventListener("dispose",pt),Se=new Map,k.programs=Se);let Ke=Se.get(Me);if(Ke!==void 0){if(k.currentProgram===Ke&&k.lightsStateVersion===pe)return B_(x,ue),Ke}else ue.uniforms=re.getUniforms(x),x.onBeforeCompile(ue,E),Ke=re.acquireProgram(ue,Me),Se.set(Me,Ke),k.uniforms=ue.uniforms;let we=k.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(we.clippingPlanes=ee.uniform),B_(x,ue),k.needsLights=tC(x),k.lightsStateVersion=pe,k.needsLights&&(we.ambientLightColor.value=F.state.ambient,we.lightProbe.value=F.state.probe,we.directionalLights.value=F.state.directional,we.directionalLightShadows.value=F.state.directionalShadow,we.spotLights.value=F.state.spot,we.spotLightShadows.value=F.state.spotShadow,we.rectAreaLights.value=F.state.rectArea,we.ltc_1.value=F.state.rectAreaLTC1,we.ltc_2.value=F.state.rectAreaLTC2,we.pointLights.value=F.state.point,we.pointLightShadows.value=F.state.pointShadow,we.hemisphereLights.value=F.state.hemi,we.directionalShadowMatrix.value=F.state.directionalShadowMatrix,we.spotLightMatrix.value=F.state.spotLightMatrix,we.spotLightMap.value=F.state.spotLightMap,we.pointShadowMatrix.value=F.state.pointShadowMatrix),k.currentProgram=Ke,k.uniformsList=null,Ke}function U_(x){if(x.uniformsList===null){let O=x.currentProgram.getUniforms();x.uniformsList=Oa.seqWithValue(O.seq,x.uniforms)}return x.uniformsList}function B_(x,O){let z=v.get(x);z.outputColorSpace=O.outputColorSpace,z.batching=O.batching,z.batchingColor=O.batchingColor,z.instancing=O.instancing,z.instancingColor=O.instancingColor,z.instancingMorph=O.instancingMorph,z.skinning=O.skinning,z.morphTargets=O.morphTargets,z.morphNormals=O.morphNormals,z.morphColors=O.morphColors,z.morphTargetsCount=O.morphTargetsCount,z.numClippingPlanes=O.numClippingPlanes,z.numIntersection=O.numClipIntersection,z.vertexAlphas=O.vertexAlphas,z.vertexTangents=O.vertexTangents,z.toneMapping=O.toneMapping}function Qw(x,O,z,k,F){O.isScene!==!0&&(O=Mt),N.resetTextureUnits();let le=O.fog,pe=k.isMeshStandardMaterial||k.isMeshLambertMaterial||k.isMeshPhongMaterial?O.environment:null,ue=V===null?E.outputColorSpace:V.isXRRenderTarget===!0?V.texture.colorSpace:Js,Me=k.isMeshStandardMaterial||k.isMeshLambertMaterial&&!k.envMap||k.isMeshPhongMaterial&&!k.envMap,Se=$.get(k.envMap||pe,Me),He=k.vertexColors===!0&&!!z.attributes.color&&z.attributes.color.itemSize===4,Ke=!!z.attributes.tangent&&(!!k.normalMap||k.anisotropy>0),we=!!z.morphAttributes.position,gt=!!z.morphAttributes.normal,Ot=!!z.morphAttributes.color,At=_i;k.toneMapped&&(V===null||V.isXRRenderTarget===!0)&&(At=E.toneMapping);let vt=z.morphAttributes.position||z.morphAttributes.normal||z.morphAttributes.color,ln=vt!==void 0?vt.length:0,Ee=v.get(k),Rn=T.state.lights;if(Ie===!0&&(Le===!0||x!==B)){let Yt=x===B&&k.id===j;ee.setState(k,x,Yt)}let st=!1;k.version===Ee.__version?(Ee.needsLights&&Ee.lightsStateVersion!==Rn.state.version||Ee.outputColorSpace!==ue||F.isBatchedMesh&&Ee.batching===!1||!F.isBatchedMesh&&Ee.batching===!0||F.isBatchedMesh&&Ee.batchingColor===!0&&F.colorTexture===null||F.isBatchedMesh&&Ee.batchingColor===!1&&F.colorTexture!==null||F.isInstancedMesh&&Ee.instancing===!1||!F.isInstancedMesh&&Ee.instancing===!0||F.isSkinnedMesh&&Ee.skinning===!1||!F.isSkinnedMesh&&Ee.skinning===!0||F.isInstancedMesh&&Ee.instancingColor===!0&&F.instanceColor===null||F.isInstancedMesh&&Ee.instancingColor===!1&&F.instanceColor!==null||F.isInstancedMesh&&Ee.instancingMorph===!0&&F.morphTexture===null||F.isInstancedMesh&&Ee.instancingMorph===!1&&F.morphTexture!==null||Ee.envMap!==Se||k.fog===!0&&Ee.fog!==le||Ee.numClippingPlanes!==void 0&&(Ee.numClippingPlanes!==ee.numPlanes||Ee.numIntersection!==ee.numIntersection)||Ee.vertexAlphas!==He||Ee.vertexTangents!==Ke||Ee.morphTargets!==we||Ee.morphNormals!==gt||Ee.morphColors!==Ot||Ee.toneMapping!==At||Ee.morphTargetsCount!==ln)&&(st=!0):(st=!0,Ee.__version=k.version);let ri=Ee.currentProgram;st===!0&&(ri=zl(k,O,F));let bi=!1,fs=!1,uo=!1,bt=ri.getUniforms(),tn=Ee.uniforms;if(be.useProgram(ri.program)&&(bi=!0,fs=!0,uo=!0),k.id!==j&&(j=k.id,fs=!0),bi||B!==x){be.buffers.depth.getReversed()&&x.reversedDepth!==!0&&(x._reversedDepth=!0,x.updateProjectionMatrix()),bt.setValue(A,"projectionMatrix",x.projectionMatrix),bt.setValue(A,"viewMatrix",x.matrixWorldInverse);let Dr=bt.map.cameraPosition;Dr!==void 0&&Dr.setValue(A,it.setFromMatrixPosition(x.matrixWorld)),Et.logarithmicDepthBuffer&&bt.setValue(A,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(k.isMeshPhongMaterial||k.isMeshToonMaterial||k.isMeshLambertMaterial||k.isMeshBasicMaterial||k.isMeshStandardMaterial||k.isShaderMaterial)&&bt.setValue(A,"isOrthographic",x.isOrthographicCamera===!0),B!==x&&(B=x,fs=!0,uo=!0)}if(Ee.needsLights&&(Rn.state.directionalShadowMap.length>0&&bt.setValue(A,"directionalShadowMap",Rn.state.directionalShadowMap,N),Rn.state.spotShadowMap.length>0&&bt.setValue(A,"spotShadowMap",Rn.state.spotShadowMap,N),Rn.state.pointShadowMap.length>0&&bt.setValue(A,"pointShadowMap",Rn.state.pointShadowMap,N)),F.isSkinnedMesh){bt.setOptional(A,F,"bindMatrix"),bt.setOptional(A,F,"bindMatrixInverse");let Yt=F.skeleton;Yt&&(Yt.boneTexture===null&&Yt.computeBoneTexture(),bt.setValue(A,"boneTexture",Yt.boneTexture,N))}F.isBatchedMesh&&(bt.setOptional(A,F,"batchingTexture"),bt.setValue(A,"batchingTexture",F._matricesTexture,N),bt.setOptional(A,F,"batchingIdTexture"),bt.setValue(A,"batchingIdTexture",F._indirectTexture,N),bt.setOptional(A,F,"batchingColorTexture"),F._colorsTexture!==null&&bt.setValue(A,"batchingColorTexture",F._colorsTexture,N));let Cr=z.morphAttributes;if((Cr.position!==void 0||Cr.normal!==void 0||Cr.color!==void 0)&&fe.update(F,z,ri),(fs||Ee.receiveShadow!==F.receiveShadow)&&(Ee.receiveShadow=F.receiveShadow,bt.setValue(A,"receiveShadow",F.receiveShadow)),(k.isMeshStandardMaterial||k.isMeshLambertMaterial||k.isMeshPhongMaterial)&&k.envMap===null&&O.environment!==null&&(tn.envMapIntensity.value=O.environmentIntensity),tn.dfgLUT!==void 0&&(tn.dfgLUT.value=B2()),fs&&(bt.setValue(A,"toneMappingExposure",E.toneMappingExposure),Ee.needsLights&&eC(tn,uo),le&&k.fog===!0&&De.refreshFogUniforms(tn,le),De.refreshMaterialUniforms(tn,k,$e,he,T.state.transmissionRenderTarget[x.id]),Oa.upload(A,U_(Ee),tn,N)),k.isShaderMaterial&&k.uniformsNeedUpdate===!0&&(Oa.upload(A,U_(Ee),tn,N),k.uniformsNeedUpdate=!1),k.isSpriteMaterial&&bt.setValue(A,"center",F.center),bt.setValue(A,"modelViewMatrix",F.modelViewMatrix),bt.setValue(A,"normalMatrix",F.normalMatrix),bt.setValue(A,"modelMatrix",F.matrixWorld),k.isShaderMaterial||k.isRawShaderMaterial){let Yt=k.uniformsGroups;for(let Dr=0,fo=Yt.length;Dr<fo;Dr++){let V_=Yt[Dr];me.update(V_,ri),me.bind(V_,ri)}}return ri}function eC(x,O){x.ambientLightColor.needsUpdate=O,x.lightProbe.needsUpdate=O,x.directionalLights.needsUpdate=O,x.directionalLightShadows.needsUpdate=O,x.pointLights.needsUpdate=O,x.pointLightShadows.needsUpdate=O,x.spotLights.needsUpdate=O,x.spotLightShadows.needsUpdate=O,x.rectAreaLights.needsUpdate=O,x.hemisphereLights.needsUpdate=O}function tC(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return C},this.getActiveMipmapLevel=function(){return U},this.getRenderTarget=function(){return V},this.setRenderTargetTextures=function(x,O,z){let k=v.get(x);k.__autoAllocateDepthBuffer=x.resolveDepthBuffer===!1,k.__autoAllocateDepthBuffer===!1&&(k.__useRenderToTexture=!1),v.get(x.texture).__webglTexture=O,v.get(x.depthTexture).__webglTexture=k.__autoAllocateDepthBuffer?void 0:z,k.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(x,O){let z=v.get(x);z.__webglFramebuffer=O,z.__useDefaultFramebuffer=O===void 0};let nC=A.createFramebuffer();this.setRenderTarget=function(x,O=0,z=0){V=x,C=O,U=z;let k=null,F=!1,le=!1;if(x){let ue=v.get(x);if(ue.__useDefaultFramebuffer!==void 0){be.bindFramebuffer(A.FRAMEBUFFER,ue.__webglFramebuffer),H.copy(x.viewport),L.copy(x.scissor),Q=x.scissorTest,be.viewport(H),be.scissor(L),be.setScissorTest(Q),j=-1;return}else if(ue.__webglFramebuffer===void 0)N.setupRenderTarget(x);else if(ue.__hasExternalTextures)N.rebindTextures(x,v.get(x.texture).__webglTexture,v.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let He=x.depthTexture;if(ue.__boundDepthTexture!==He){if(He!==null&&v.has(He)&&(x.width!==He.image.width||x.height!==He.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");N.setupDepthRenderbuffer(x)}}let Me=x.texture;(Me.isData3DTexture||Me.isDataArrayTexture||Me.isCompressedArrayTexture)&&(le=!0);let Se=v.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(Se[O])?k=Se[O][z]:k=Se[O],F=!0):x.samples>0&&N.useMultisampledRTT(x)===!1?k=v.get(x).__webglMultisampledFramebuffer:Array.isArray(Se)?k=Se[z]:k=Se,H.copy(x.viewport),L.copy(x.scissor),Q=x.scissorTest}else H.copy(X).multiplyScalar($e).floor(),L.copy(ne).multiplyScalar($e).floor(),Q=oe;if(z!==0&&(k=nC),be.bindFramebuffer(A.FRAMEBUFFER,k)&&be.drawBuffers(x,k),be.viewport(H),be.scissor(L),be.setScissorTest(Q),F){let ue=v.get(x.texture);A.framebufferTexture2D(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_CUBE_MAP_POSITIVE_X+O,ue.__webglTexture,z)}else if(le){let ue=O;for(let Me=0;Me<x.textures.length;Me++){let Se=v.get(x.textures[Me]);A.framebufferTextureLayer(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0+Me,Se.__webglTexture,z,ue)}}else if(x!==null&&z!==0){let ue=v.get(x.texture);A.framebufferTexture2D(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,ue.__webglTexture,z)}j=-1},this.readRenderTargetPixels=function(x,O,z,k,F,le,pe,ue=0){if(!(x&&x.isWebGLRenderTarget)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let Me=v.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&pe!==void 0&&(Me=Me[pe]),Me){be.bindFramebuffer(A.FRAMEBUFFER,Me);try{let Se=x.textures[ue],He=Se.format,Ke=Se.type;if(x.textures.length>1&&A.readBuffer(A.COLOR_ATTACHMENT0+ue),!Et.textureFormatReadable(He)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Et.textureTypeReadable(Ke)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}O>=0&&O<=x.width-k&&z>=0&&z<=x.height-F&&A.readPixels(O,z,k,F,se.convert(He),se.convert(Ke),le)}finally{let Se=V!==null?v.get(V).__webglFramebuffer:null;be.bindFramebuffer(A.FRAMEBUFFER,Se)}}},this.readRenderTargetPixelsAsync=async function(x,O,z,k,F,le,pe,ue=0){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let Me=v.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&pe!==void 0&&(Me=Me[pe]),Me)if(O>=0&&O<=x.width-k&&z>=0&&z<=x.height-F){be.bindFramebuffer(A.FRAMEBUFFER,Me);let Se=x.textures[ue],He=Se.format,Ke=Se.type;if(x.textures.length>1&&A.readBuffer(A.COLOR_ATTACHMENT0+ue),!Et.textureFormatReadable(He))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Et.textureTypeReadable(Ke))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let we=A.createBuffer();A.bindBuffer(A.PIXEL_PACK_BUFFER,we),A.bufferData(A.PIXEL_PACK_BUFFER,le.byteLength,A.STREAM_READ),A.readPixels(O,z,k,F,se.convert(He),se.convert(Ke),0);let gt=V!==null?v.get(V).__webglFramebuffer:null;be.bindFramebuffer(A.FRAMEBUFFER,gt);let Ot=A.fenceSync(A.SYNC_GPU_COMMANDS_COMPLETE,0);return A.flush(),await vw(A,Ot,4),A.bindBuffer(A.PIXEL_PACK_BUFFER,we),A.getBufferSubData(A.PIXEL_PACK_BUFFER,0,le),A.deleteBuffer(we),A.deleteSync(Ot),le}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(x,O=null,z=0){let k=Math.pow(2,-z),F=Math.floor(x.image.width*k),le=Math.floor(x.image.height*k),pe=O!==null?O.x:0,ue=O!==null?O.y:0;N.setTexture2D(x,0),A.copyTexSubImage2D(A.TEXTURE_2D,z,0,0,pe,ue,F,le),be.unbindTexture()};let iC=A.createFramebuffer(),rC=A.createFramebuffer();this.copyTextureToTexture=function(x,O,z=null,k=null,F=0,le=0){let pe,ue,Me,Se,He,Ke,we,gt,Ot,At=x.isCompressedTexture?x.mipmaps[le]:x.image;if(z!==null)pe=z.max.x-z.min.x,ue=z.max.y-z.min.y,Me=z.isBox3?z.max.z-z.min.z:1,Se=z.min.x,He=z.min.y,Ke=z.isBox3?z.min.z:0;else{let tn=Math.pow(2,-F);pe=Math.floor(At.width*tn),ue=Math.floor(At.height*tn),x.isDataArrayTexture?Me=At.depth:x.isData3DTexture?Me=Math.floor(At.depth*tn):Me=1,Se=0,He=0,Ke=0}k!==null?(we=k.x,gt=k.y,Ot=k.z):(we=0,gt=0,Ot=0);let vt=se.convert(O.format),ln=se.convert(O.type),Ee;O.isData3DTexture?(N.setTexture3D(O,0),Ee=A.TEXTURE_3D):O.isDataArrayTexture||O.isCompressedArrayTexture?(N.setTexture2DArray(O,0),Ee=A.TEXTURE_2D_ARRAY):(N.setTexture2D(O,0),Ee=A.TEXTURE_2D),A.pixelStorei(A.UNPACK_FLIP_Y_WEBGL,O.flipY),A.pixelStorei(A.UNPACK_PREMULTIPLY_ALPHA_WEBGL,O.premultiplyAlpha),A.pixelStorei(A.UNPACK_ALIGNMENT,O.unpackAlignment);let Rn=A.getParameter(A.UNPACK_ROW_LENGTH),st=A.getParameter(A.UNPACK_IMAGE_HEIGHT),ri=A.getParameter(A.UNPACK_SKIP_PIXELS),bi=A.getParameter(A.UNPACK_SKIP_ROWS),fs=A.getParameter(A.UNPACK_SKIP_IMAGES);A.pixelStorei(A.UNPACK_ROW_LENGTH,At.width),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,At.height),A.pixelStorei(A.UNPACK_SKIP_PIXELS,Se),A.pixelStorei(A.UNPACK_SKIP_ROWS,He),A.pixelStorei(A.UNPACK_SKIP_IMAGES,Ke);let uo=x.isDataArrayTexture||x.isData3DTexture,bt=O.isDataArrayTexture||O.isData3DTexture;if(x.isDepthTexture){let tn=v.get(x),Cr=v.get(O),Yt=v.get(tn.__renderTarget),Dr=v.get(Cr.__renderTarget);be.bindFramebuffer(A.READ_FRAMEBUFFER,Yt.__webglFramebuffer),be.bindFramebuffer(A.DRAW_FRAMEBUFFER,Dr.__webglFramebuffer);for(let fo=0;fo<Me;fo++)uo&&(A.framebufferTextureLayer(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,v.get(x).__webglTexture,F,Ke+fo),A.framebufferTextureLayer(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,v.get(O).__webglTexture,le,Ot+fo)),A.blitFramebuffer(Se,He,pe,ue,we,gt,pe,ue,A.DEPTH_BUFFER_BIT,A.NEAREST);be.bindFramebuffer(A.READ_FRAMEBUFFER,null),be.bindFramebuffer(A.DRAW_FRAMEBUFFER,null)}else if(F!==0||x.isRenderTargetTexture||v.has(x)){let tn=v.get(x),Cr=v.get(O);be.bindFramebuffer(A.READ_FRAMEBUFFER,iC),be.bindFramebuffer(A.DRAW_FRAMEBUFFER,rC);for(let Yt=0;Yt<Me;Yt++)uo?A.framebufferTextureLayer(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,tn.__webglTexture,F,Ke+Yt):A.framebufferTexture2D(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,tn.__webglTexture,F),bt?A.framebufferTextureLayer(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,Cr.__webglTexture,le,Ot+Yt):A.framebufferTexture2D(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,Cr.__webglTexture,le),F!==0?A.blitFramebuffer(Se,He,pe,ue,we,gt,pe,ue,A.COLOR_BUFFER_BIT,A.NEAREST):bt?A.copyTexSubImage3D(Ee,le,we,gt,Ot+Yt,Se,He,pe,ue):A.copyTexSubImage2D(Ee,le,we,gt,Se,He,pe,ue);be.bindFramebuffer(A.READ_FRAMEBUFFER,null),be.bindFramebuffer(A.DRAW_FRAMEBUFFER,null)}else bt?x.isDataTexture||x.isData3DTexture?A.texSubImage3D(Ee,le,we,gt,Ot,pe,ue,Me,vt,ln,At.data):O.isCompressedArrayTexture?A.compressedTexSubImage3D(Ee,le,we,gt,Ot,pe,ue,Me,vt,At.data):A.texSubImage3D(Ee,le,we,gt,Ot,pe,ue,Me,vt,ln,At):x.isDataTexture?A.texSubImage2D(A.TEXTURE_2D,le,we,gt,pe,ue,vt,ln,At.data):x.isCompressedTexture?A.compressedTexSubImage2D(A.TEXTURE_2D,le,we,gt,At.width,At.height,vt,At.data):A.texSubImage2D(A.TEXTURE_2D,le,we,gt,pe,ue,vt,ln,At);A.pixelStorei(A.UNPACK_ROW_LENGTH,Rn),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,st),A.pixelStorei(A.UNPACK_SKIP_PIXELS,ri),A.pixelStorei(A.UNPACK_SKIP_ROWS,bi),A.pixelStorei(A.UNPACK_SKIP_IMAGES,fs),le===0&&O.generateMipmaps&&A.generateMipmap(Ee),be.unbindTexture()},this.initRenderTarget=function(x){v.get(x).__webglFramebuffer===void 0&&N.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?N.setTextureCube(x,0):x.isData3DTexture?N.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?N.setTexture2DArray(x,0):N.setTexture2D(x,0),be.unbindTexture()},this.resetState=function(){C=0,U=0,V=null,be.reset(),te.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return yi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=nt._getDrawingBufferColorSpace(e),t.unpackColorSpace=nt._getUnpackColorSpace()}};var Yw={type:"change"},R_={type:"start"},Kw={type:"end"},dp=new to,Zw=new ni,H2=Math.cos(70*u_.DEG2RAD),qt=new P,In=2*Math.PI,xt={NONE:-1,ROTATE:0,DOLLY:1,PAN:2,TOUCH_ROTATE:3,TOUCH_PAN:4,TOUCH_DOLLY_PAN:5,TOUCH_DOLLY_ROTATE:6},I_=1e-6,fp=class extends Il{constructor(e,t=null){super(e,t),this.state=xt.NONE,this.target=new P,this.cursor=new P,this.minDistance=0,this.maxDistance=1/0,this.minZoom=0,this.maxZoom=1/0,this.minTargetRadius=0,this.maxTargetRadius=1/0,this.minPolarAngle=0,this.maxPolarAngle=Math.PI,this.minAzimuthAngle=-1/0,this.maxAzimuthAngle=1/0,this.enableDamping=!1,this.dampingFactor=.05,this.enableZoom=!0,this.zoomSpeed=1,this.enableRotate=!0,this.rotateSpeed=1,this.keyRotateSpeed=1,this.enablePan=!0,this.panSpeed=1,this.screenSpacePanning=!0,this.keyPanSpeed=7,this.zoomToCursor=!1,this.autoRotate=!1,this.autoRotateSpeed=2,this.keys={LEFT:"ArrowLeft",UP:"ArrowUp",RIGHT:"ArrowRight",BOTTOM:"ArrowDown"},this.mouseButtons={LEFT:ss.ROTATE,MIDDLE:ss.DOLLY,RIGHT:ss.PAN},this.touches={ONE:os.ROTATE,TWO:os.DOLLY_PAN},this.target0=this.target.clone(),this.position0=this.object.position.clone(),this.zoom0=this.object.zoom,this._cursorStyle="auto",this._domElementKeyEvents=null,this._lastPosition=new P,this._lastQuaternion=new zn,this._lastTargetPosition=new P,this._quat=new zn().setFromUnitVectors(e.up,new P(0,1,0)),this._quatInverse=this._quat.clone().invert(),this._spherical=new Aa,this._sphericalDelta=new Aa,this._scale=1,this._panOffset=new P,this._rotateStart=new Ne,this._rotateEnd=new Ne,this._rotateDelta=new Ne,this._panStart=new Ne,this._panEnd=new Ne,this._panDelta=new Ne,this._dollyStart=new Ne,this._dollyEnd=new Ne,this._dollyDelta=new Ne,this._dollyDirection=new P,this._mouse=new Ne,this._performCursorZoom=!1,this._pointers=[],this._pointerPositions={},this._controlActive=!1,this._onPointerMove=G2.bind(this),this._onPointerDown=z2.bind(this),this._onPointerUp=j2.bind(this),this._onContextMenu=K2.bind(this),this._onMouseWheel=q2.bind(this),this._onKeyDown=X2.bind(this),this._onTouchStart=Y2.bind(this),this._onTouchMove=Z2.bind(this),this._onMouseDown=W2.bind(this),this._onMouseMove=$2.bind(this),this._interceptControlDown=J2.bind(this),this._interceptControlUp=Q2.bind(this),this.domElement!==null&&this.connect(this.domElement),this.update()}set cursorStyle(e){this._cursorStyle=e,e==="grab"?this.domElement.style.cursor="grab":this.domElement.style.cursor="auto"}get cursorStyle(){return this._cursorStyle}connect(e){super.connect(e),this.domElement.addEventListener("pointerdown",this._onPointerDown),this.domElement.addEventListener("pointercancel",this._onPointerUp),this.domElement.addEventListener("contextmenu",this._onContextMenu),this.domElement.addEventListener("wheel",this._onMouseWheel,{passive:!1}),this.domElement.getRootNode().addEventListener("keydown",this._interceptControlDown,{passive:!0,capture:!0}),this.domElement.style.touchAction="none"}disconnect(){this.domElement.removeEventListener("pointerdown",this._onPointerDown),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.domElement.removeEventListener("pointercancel",this._onPointerUp),this.domElement.removeEventListener("wheel",this._onMouseWheel),this.domElement.removeEventListener("contextmenu",this._onContextMenu),this.stopListenToKeyEvents(),this.domElement.getRootNode().removeEventListener("keydown",this._interceptControlDown,{capture:!0}),this.domElement.style.touchAction="auto"}dispose(){this.disconnect()}getPolarAngle(){return this._spherical.phi}getAzimuthalAngle(){return this._spherical.theta}getDistance(){return this.object.position.distanceTo(this.target)}listenToKeyEvents(e){e.addEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=e}stopListenToKeyEvents(){this._domElementKeyEvents!==null&&(this._domElementKeyEvents.removeEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=null)}saveState(){this.target0.copy(this.target),this.position0.copy(this.object.position),this.zoom0=this.object.zoom}reset(){this.target.copy(this.target0),this.object.position.copy(this.position0),this.object.zoom=this.zoom0,this.object.updateProjectionMatrix(),this.dispatchEvent(Yw),this.update(),this.state=xt.NONE}pan(e,t){this._pan(e,t),this.update()}dollyIn(e){this._dollyIn(e),this.update()}dollyOut(e){this._dollyOut(e),this.update()}rotateLeft(e){this._rotateLeft(e),this.update()}rotateUp(e){this._rotateUp(e),this.update()}update(e=null){let t=this.object.position;qt.copy(t).sub(this.target),qt.applyQuaternion(this._quat),this._spherical.setFromVector3(qt),this.autoRotate&&this.state===xt.NONE&&this._rotateLeft(this._getAutoRotationAngle(e)),this.enableDamping?(this._spherical.theta+=this._sphericalDelta.theta*this.dampingFactor,this._spherical.phi+=this._sphericalDelta.phi*this.dampingFactor):(this._spherical.theta+=this._sphericalDelta.theta,this._spherical.phi+=this._sphericalDelta.phi);let i=this.minAzimuthAngle,r=this.maxAzimuthAngle;isFinite(i)&&isFinite(r)&&(i<-Math.PI?i+=In:i>Math.PI&&(i-=In),r<-Math.PI?r+=In:r>Math.PI&&(r-=In),i<=r?this._spherical.theta=Math.max(i,Math.min(r,this._spherical.theta)):this._spherical.theta=this._spherical.theta>(i+r)/2?Math.max(i,this._spherical.theta):Math.min(r,this._spherical.theta)),this._spherical.phi=Math.max(this.minPolarAngle,Math.min(this.maxPolarAngle,this._spherical.phi)),this._spherical.makeSafe(),this.enableDamping===!0?this.target.addScaledVector(this._panOffset,this.dampingFactor):this.target.add(this._panOffset),this.target.sub(this.cursor),this.target.clampLength(this.minTargetRadius,this.maxTargetRadius),this.target.add(this.cursor);let s=!1;if(this.zoomToCursor&&this._performCursorZoom||this.object.isOrthographicCamera)this._spherical.radius=this._clampDistance(this._spherical.radius);else{let o=this._spherical.radius;this._spherical.radius=this._clampDistance(this._spherical.radius*this._scale),s=o!=this._spherical.radius}if(qt.setFromSpherical(this._spherical),qt.applyQuaternion(this._quatInverse),t.copy(this.target).add(qt),this.object.lookAt(this.target),this.enableDamping===!0?(this._sphericalDelta.theta*=1-this.dampingFactor,this._sphericalDelta.phi*=1-this.dampingFactor,this._panOffset.multiplyScalar(1-this.dampingFactor)):(this._sphericalDelta.set(0,0,0),this._panOffset.set(0,0,0)),this.zoomToCursor&&this._performCursorZoom){let o=null;if(this.object.isPerspectiveCamera){let a=qt.length();o=this._clampDistance(a*this._scale);let c=a-o;this.object.position.addScaledVector(this._dollyDirection,c),this.object.updateMatrixWorld(),s=!!c}else if(this.object.isOrthographicCamera){let a=new P(this._mouse.x,this._mouse.y,0);a.unproject(this.object);let c=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),this.object.updateProjectionMatrix(),s=c!==this.object.zoom;let l=new P(this._mouse.x,this._mouse.y,0);l.unproject(this.object),this.object.position.sub(l).add(a),this.object.updateMatrixWorld(),o=qt.length()}else console.warn("WARNING: OrbitControls.js encountered an unknown camera type - zoom to cursor disabled."),this.zoomToCursor=!1;o!==null&&(this.screenSpacePanning?this.target.set(0,0,-1).transformDirection(this.object.matrix).multiplyScalar(o).add(this.object.position):(dp.origin.copy(this.object.position),dp.direction.set(0,0,-1).transformDirection(this.object.matrix),Math.abs(this.object.up.dot(dp.direction))<H2?this.object.lookAt(this.target):(Zw.setFromNormalAndCoplanarPoint(this.object.up,this.target),dp.intersectPlane(Zw,this.target))))}else if(this.object.isOrthographicCamera){let o=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),o!==this.object.zoom&&(this.object.updateProjectionMatrix(),s=!0)}return this._scale=1,this._performCursorZoom=!1,s||this._lastPosition.distanceToSquared(this.object.position)>I_||8*(1-this._lastQuaternion.dot(this.object.quaternion))>I_||this._lastTargetPosition.distanceToSquared(this.target)>I_?(this.dispatchEvent(Yw),this._lastPosition.copy(this.object.position),this._lastQuaternion.copy(this.object.quaternion),this._lastTargetPosition.copy(this.target),!0):!1}_getAutoRotationAngle(e){return e!==null?In/60*this.autoRotateSpeed*e:In/60/60*this.autoRotateSpeed}_getZoomScale(e){let t=Math.abs(e*.01);return Math.pow(.95,this.zoomSpeed*t)}_rotateLeft(e){this._sphericalDelta.theta-=e}_rotateUp(e){this._sphericalDelta.phi-=e}_panLeft(e,t){qt.setFromMatrixColumn(t,0),qt.multiplyScalar(-e),this._panOffset.add(qt)}_panUp(e,t){this.screenSpacePanning===!0?qt.setFromMatrixColumn(t,1):(qt.setFromMatrixColumn(t,0),qt.crossVectors(this.object.up,qt)),qt.multiplyScalar(e),this._panOffset.add(qt)}_pan(e,t){let i=this.domElement;if(this.object.isPerspectiveCamera){let r=this.object.position;qt.copy(r).sub(this.target);let s=qt.length();s*=Math.tan(this.object.fov/2*Math.PI/180),this._panLeft(2*e*s/i.clientHeight,this.object.matrix),this._panUp(2*t*s/i.clientHeight,this.object.matrix)}else this.object.isOrthographicCamera?(this._panLeft(e*(this.object.right-this.object.left)/this.object.zoom/i.clientWidth,this.object.matrix),this._panUp(t*(this.object.top-this.object.bottom)/this.object.zoom/i.clientHeight,this.object.matrix)):(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - pan disabled."),this.enablePan=!1)}_dollyOut(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale/=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_dollyIn(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale*=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_updateZoomParameters(e,t){if(!this.zoomToCursor)return;this._performCursorZoom=!0;let i=this.domElement.getBoundingClientRect(),r=e-i.left,s=t-i.top,o=i.width,a=i.height;this._mouse.x=r/o*2-1,this._mouse.y=-(s/a)*2+1,this._dollyDirection.set(this._mouse.x,this._mouse.y,1).unproject(this.object).sub(this.object.position).normalize()}_clampDistance(e){return Math.max(this.minDistance,Math.min(this.maxDistance,e))}_handleMouseDownRotate(e){this._rotateStart.set(e.clientX,e.clientY)}_handleMouseDownDolly(e){this._updateZoomParameters(e.clientX,e.clientX),this._dollyStart.set(e.clientX,e.clientY)}_handleMouseDownPan(e){this._panStart.set(e.clientX,e.clientY)}_handleMouseMoveRotate(e){this._rotateEnd.set(e.clientX,e.clientY),this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(In*this._rotateDelta.x/t.clientHeight),this._rotateUp(In*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd),this.update()}_handleMouseMoveDolly(e){this._dollyEnd.set(e.clientX,e.clientY),this._dollyDelta.subVectors(this._dollyEnd,this._dollyStart),this._dollyDelta.y>0?this._dollyOut(this._getZoomScale(this._dollyDelta.y)):this._dollyDelta.y<0&&this._dollyIn(this._getZoomScale(this._dollyDelta.y)),this._dollyStart.copy(this._dollyEnd),this.update()}_handleMouseMovePan(e){this._panEnd.set(e.clientX,e.clientY),this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd),this.update()}_handleMouseWheel(e){this._updateZoomParameters(e.clientX,e.clientY),e.deltaY<0?this._dollyIn(this._getZoomScale(e.deltaY)):e.deltaY>0&&this._dollyOut(this._getZoomScale(e.deltaY)),this.update()}_handleKeyDown(e){let t=!1;switch(e.code){case this.keys.UP:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(In*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,this.keyPanSpeed),t=!0;break;case this.keys.BOTTOM:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(-In*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,-this.keyPanSpeed),t=!0;break;case this.keys.LEFT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(In*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(this.keyPanSpeed,0),t=!0;break;case this.keys.RIGHT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(-In*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(-this.keyPanSpeed,0),t=!0;break}t&&(e.preventDefault(),this.update())}_handleTouchStartRotate(e){if(this._pointers.length===1)this._rotateStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._rotateStart.set(i,r)}}_handleTouchStartPan(e){if(this._pointers.length===1)this._panStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panStart.set(i,r)}}_handleTouchStartDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyStart.set(0,s)}_handleTouchStartDollyPan(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enablePan&&this._handleTouchStartPan(e)}_handleTouchStartDollyRotate(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enableRotate&&this._handleTouchStartRotate(e)}_handleTouchMoveRotate(e){if(this._pointers.length==1)this._rotateEnd.set(e.pageX,e.pageY);else{let i=this._getSecondPointerPosition(e),r=.5*(e.pageX+i.x),s=.5*(e.pageY+i.y);this._rotateEnd.set(r,s)}this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(In*this._rotateDelta.x/t.clientHeight),this._rotateUp(In*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd)}_handleTouchMovePan(e){if(this._pointers.length===1)this._panEnd.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panEnd.set(i,r)}this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd)}_handleTouchMoveDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyEnd.set(0,s),this._dollyDelta.set(0,Math.pow(this._dollyEnd.y/this._dollyStart.y,this.zoomSpeed)),this._dollyOut(this._dollyDelta.y),this._dollyStart.copy(this._dollyEnd);let o=(e.pageX+t.x)*.5,a=(e.pageY+t.y)*.5;this._updateZoomParameters(o,a)}_handleTouchMoveDollyPan(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enablePan&&this._handleTouchMovePan(e)}_handleTouchMoveDollyRotate(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enableRotate&&this._handleTouchMoveRotate(e)}_addPointer(e){this._pointers.push(e.pointerId)}_removePointer(e){delete this._pointerPositions[e.pointerId];for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId){this._pointers.splice(t,1);return}}_isTrackingPointer(e){for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId)return!0;return!1}_trackPointer(e){let t=this._pointerPositions[e.pointerId];t===void 0&&(t=new Ne,this._pointerPositions[e.pointerId]=t),t.set(e.pageX,e.pageY)}_getSecondPointerPosition(e){let t=e.pointerId===this._pointers[0]?this._pointers[1]:this._pointers[0];return this._pointerPositions[t]}_customWheelEvent(e){let t=e.deltaMode,i={clientX:e.clientX,clientY:e.clientY,deltaY:e.deltaY};switch(t){case 1:i.deltaY*=16;break;case 2:i.deltaY*=100;break}return e.ctrlKey&&!this._controlActive&&(i.deltaY*=10),i}};function z2(n){this.enabled!==!1&&(this._pointers.length===0&&(this.domElement.setPointerCapture(n.pointerId),this.domElement.ownerDocument.addEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.addEventListener("pointerup",this._onPointerUp)),!this._isTrackingPointer(n)&&(this._addPointer(n),n.pointerType==="touch"?this._onTouchStart(n):this._onMouseDown(n),this._cursorStyle==="grab"&&(this.domElement.style.cursor="grabbing")))}function G2(n){this.enabled!==!1&&(n.pointerType==="touch"?this._onTouchMove(n):this._onMouseMove(n))}function j2(n){switch(this._removePointer(n),this._pointers.length){case 0:this.domElement.releasePointerCapture(n.pointerId),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.dispatchEvent(Kw),this.state=xt.NONE,this._cursorStyle==="grab"&&(this.domElement.style.cursor="grab");break;case 1:let e=this._pointers[0],t=this._pointerPositions[e];this._onTouchStart({pointerId:e,pageX:t.x,pageY:t.y});break}}function W2(n){let e;switch(n.button){case 0:e=this.mouseButtons.LEFT;break;case 1:e=this.mouseButtons.MIDDLE;break;case 2:e=this.mouseButtons.RIGHT;break;default:e=-1}switch(e){case ss.DOLLY:if(this.enableZoom===!1)return;this._handleMouseDownDolly(n),this.state=xt.DOLLY;break;case ss.ROTATE:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=xt.PAN}else{if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=xt.ROTATE}break;case ss.PAN:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=xt.ROTATE}else{if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=xt.PAN}break;default:this.state=xt.NONE}this.state!==xt.NONE&&this.dispatchEvent(R_)}function $2(n){switch(this.state){case xt.ROTATE:if(this.enableRotate===!1)return;this._handleMouseMoveRotate(n);break;case xt.DOLLY:if(this.enableZoom===!1)return;this._handleMouseMoveDolly(n);break;case xt.PAN:if(this.enablePan===!1)return;this._handleMouseMovePan(n);break}}function q2(n){this.enabled===!1||this.enableZoom===!1||this.state!==xt.NONE||(n.preventDefault(),this.dispatchEvent(R_),this._handleMouseWheel(this._customWheelEvent(n)),this.dispatchEvent(Kw))}function X2(n){this.enabled!==!1&&this._handleKeyDown(n)}function Y2(n){switch(this._trackPointer(n),this._pointers.length){case 1:switch(this.touches.ONE){case os.ROTATE:if(this.enableRotate===!1)return;this._handleTouchStartRotate(n),this.state=xt.TOUCH_ROTATE;break;case os.PAN:if(this.enablePan===!1)return;this._handleTouchStartPan(n),this.state=xt.TOUCH_PAN;break;default:this.state=xt.NONE}break;case 2:switch(this.touches.TWO){case os.DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchStartDollyPan(n),this.state=xt.TOUCH_DOLLY_PAN;break;case os.DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchStartDollyRotate(n),this.state=xt.TOUCH_DOLLY_ROTATE;break;default:this.state=xt.NONE}break;default:this.state=xt.NONE}this.state!==xt.NONE&&this.dispatchEvent(R_)}function Z2(n){switch(this._trackPointer(n),this.state){case xt.TOUCH_ROTATE:if(this.enableRotate===!1)return;this._handleTouchMoveRotate(n),this.update();break;case xt.TOUCH_PAN:if(this.enablePan===!1)return;this._handleTouchMovePan(n),this.update();break;case xt.TOUCH_DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchMoveDollyPan(n),this.update();break;case xt.TOUCH_DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchMoveDollyRotate(n),this.update();break;default:this.state=xt.NONE}}function K2(n){this.enabled!==!1&&n.preventDefault()}function J2(n){n.key==="Control"&&(this._controlActive=!0,this.domElement.getRootNode().addEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}function Q2(n){n.key==="Control"&&(this._controlActive=!1,this.domElement.getRootNode().removeEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}var eU=["rendererContainer"],tU=()=>["A","B","C"];function nU(n,e){if(n&1){let t=gd();Kt(0,"div",12)(1,"div",13)(2,"label"),Jn(3),Jt(),Kt(4,"span",14),Jn(5),Jt()(),Kt(6,"input",15),ks("ngModelChange",function(r){let s=Ai(t).$implicit,o=vd();return Bo(o.rotation[s],r)||(o.rotation[s]=r),Ii(r)}),Jt()()}if(n&2){let t=e.$implicit,i=vd();fr(3),Cc("\u041E\u0441\u044C ",t),fr(2),xd(i.rotation[t].toFixed(2)),fr(),Fs("ngModel",i.rotation[t])}}var hp=class n{constructor(e,t){this.ngZone=e;this.cdr=t}rendererContainer;rotation={A:0,B:0,C:0};newDir={a:"",b:"",c:""};scene;camera;renderer;controls;frameId=null;mainGroup;customGroup;resetRotation(){this.rotation={A:0,B:0,C:0},this.mainGroup&&this.mainGroup.rotation.set(0,0,0),this.controls.reset(),this.cdr.detectChanges()}undoLastDirection(){if(this.customGroup.children.length>0){for(let e=0;e<3;e++){let t=this.customGroup.children[this.customGroup.children.length-1];t&&(t.geometry&&t.geometry.dispose(),t.material&&t.material.dispose(),this.customGroup.remove(t))}this.cdr.detectChanges()}}addCustomDirection(){let e=parseFloat(this.newDir.a||"0"),t=parseFloat(this.newDir.b||"0"),i=parseFloat(this.newDir.c||"0");if(e===0&&t===0&&i===0)return;let r=new P(t,i,e),s=`${e}|${t}|${i}`,o=new We().setHSL(Math.random(),.8,.6),a=r.clone().normalize(),c=new wa(new vn().setFromPoints([new P(0,0,0),a.clone().multiplyScalar(4)]),new io({color:o,linewidth:2})),l=new wn(new Ta(.08,20,20),new no({color:o}));l.position.copy(a.clone().multiplyScalar(4));let u=this.createCanvasLabel(s,36,o.getHex(),!0),d=new Sa(new eo({map:new Ca(u)}));d.position.copy(a.clone().multiplyScalar(4.4)),d.scale.set(.9,.45,1),this.customGroup.add(c,l,d),this.newDir={a:"",b:"",c:""},this.cdr.detectChanges()}ngAfterViewInit(){this.ngZone.runOutsideAngular(()=>{this.initThree(),this.createSceneObjects(),this.animate()})}initThree(){this.scene=new gl,this.scene.background=new We(328965);let e=this.rendererContainer.nativeElement;this.camera=new gn(75,e.clientWidth/e.clientHeight,.1,1e3),this.camera.position.set(8,6,8),this.renderer=new cp({antialias:!0,alpha:!0}),this.renderer.setPixelRatio(window.devicePixelRatio),this.renderer.setSize(e.clientWidth,e.clientHeight),e.appendChild(this.renderer.domElement),this.scene.add(new Tl(16777215,1)),this.controls=new fp(this.camera,this.renderer.domElement),this.controls.enableDamping=!0}createSceneObjects(){this.mainGroup=new Bi,this.mainGroup.rotation.order="ZYX",this.customGroup=new Bi,this.mainGroup.add(this.customGroup),this.mainGroup.add(new wn(new Ta(3,64,64),new Sl({color:16777215,wireframe:!0,transparent:!0,opacity:.05})));let e=new Al(5);this.scene.add(e),this.addAxisLabel("A",new P(0,0,5.5),4474111),this.addAxisLabel("B",new P(5.5,0,0),16729156),this.addAxisLabel("C",new P(0,5.5,0),4521796),this.scene.add(this.mainGroup)}createCanvasLabel(e,t,i,r=!1){let s=document.createElement("canvas"),o=s.getContext("2d");s.width=512,s.height=256,o.font=`Bold ${t*2}px monospace`,o.fillStyle=`#${new We(i).getHexString()}`,o.textAlign="center",o.textBaseline="middle";let a=t*2*.7,l=256-e.replace(/-/g,"").length*a/2+a/2;for(let u=0;u<e.length;u++){let d=e[u];d==="-"?o.fillRect(l-a/2+5,128-t*1.1,a-10,8):(o.fillText(d,l,128),l+=a)}return s}addAxisLabel(e,t,i){let r=this.createCanvasLabel(e,60,i),s=new Sa(new eo({map:new Ca(r)}));s.position.copy(t),s.scale.set(.8,.4,1),this.scene.add(s)}animate(){this.frameId=requestAnimationFrame(()=>this.animate()),this.mainGroup&&(this.mainGroup.rotation.z=this.rotation.A,this.mainGroup.rotation.x=this.rotation.B,this.mainGroup.rotation.y=this.rotation.C),this.controls.update(),this.renderer.render(this.scene,this.camera)}ngOnDestroy(){this.frameId&&cancelAnimationFrame(this.frameId),this.renderer.dispose()}static \u0275fac=function(t){return new(t||n)(Tt(Mn),Tt(Us))};static \u0275cmp=Sc({type:n,selectors:[["app-root"]],viewQuery:function(t,i){if(t&1&&yd(eU,5),t&2){let r;Kg(r=Jg())&&(i.rendererContainer=r.first)}},decls:29,vars:5,consts:[["rendererContainer",""],[1,"sphere-wrapper"],[1,"ui-panel","left-panel"],["class","control",4,"ngFor","ngForOf"],[1,"reset-btn",3,"click"],[1,"ui-panel","right-panel"],[1,"input-grid"],[1,"input-col"],["type","number","placeholder","0",3,"ngModelChange","ngModel"],[1,"action-buttons"],[1,"add-btn",3,"click"],[1,"undo-btn",3,"click"],[1,"control"],[1,"label-row"],[1,"val"],["type","range","min","0","max","6.28","step","0.01",3,"ngModelChange","ngModel"]],template:function(t,i){if(t&1){let r=gd();Uo(0,"div",1,0),Kt(2,"div",2)(3,"h3"),Jn(4,"\u041D\u0430\u0432\u0438\u0433\u0430\u0446\u0438\u044F [A B C]"),Jt(),hd(5,nU,7,3,"div",3),Kt(6,"button",4),Oi("click",function(){return Ai(r),Ii(i.resetRotation())}),Jn(7,"\u0421\u0431\u0440\u043E\u0441"),Jt()(),Kt(8,"div",5)(9,"h3"),Jn(10,"\u041D\u0430\u043F\u0440\u0430\u0432\u043B\u0435\u043D\u0438\u0435"),Jt(),Kt(11,"div",6)(12,"div",7)(13,"label"),Jn(14,"A"),Jt(),Kt(15,"input",8),ks("ngModelChange",function(o){return Ai(r),Bo(i.newDir.a,o)||(i.newDir.a=o),Ii(o)}),Jt()(),Kt(16,"div",7)(17,"label"),Jn(18,"B"),Jt(),Kt(19,"input",8),ks("ngModelChange",function(o){return Ai(r),Bo(i.newDir.b,o)||(i.newDir.b=o),Ii(o)}),Jt()(),Kt(20,"div",7)(21,"label"),Jn(22,"C"),Jt(),Kt(23,"input",8),ks("ngModelChange",function(o){return Ai(r),Bo(i.newDir.c,o)||(i.newDir.c=o),Ii(o)}),Jt()()(),Kt(24,"div",9)(25,"button",10),Oi("click",function(){return Ai(r),Ii(i.addCustomDirection())}),Jn(26,"\u0414\u043E\u0431\u0430\u0432\u0438\u0442\u044C"),Jt(),Kt(27,"button",11),Oi("click",function(){return Ai(r),Ii(i.undoLastDirection())}),Jn(28,"\u041E\u0442\u043A\u0430\u0442"),Jt()()()}t&2&&(fr(5),md("ngForOf",Qg(4,tU)),fr(10),Fs("ngModel",i.newDir.a),fr(4),Fs("ngModel",i.newDir.b),fr(4),Fs("ngModel",i.newDir.c))},dependencies:[fS,sf,uy,dy,lS,ly,Dd,Cd],styles:["[_nghost-%COMP%]{display:block;width:100vw;height:100vh;margin:0;overflow:hidden;background-color:#050505}.sphere-wrapper[_ngcontent-%COMP%]{width:100%;height:100%}.ui-panel[_ngcontent-%COMP%]{position:absolute;top:24px;z-index:100;background:#121212eb;-webkit-backdrop-filter:blur(20px);backdrop-filter:blur(20px);padding:20px;border-radius:16px;color:#fff;font-family:Inter,system-ui,-apple-system,sans-serif;width:230px;box-shadow:0 12px 48px #000c;border:1px solid rgba(255,255,255,.1)}.left-panel[_ngcontent-%COMP%]{left:24px}.right-panel[_ngcontent-%COMP%]{right:24px}h3[_ngcontent-%COMP%]{margin:0 0 18px;font-size:13px;color:#0fc;text-transform:uppercase;letter-spacing:1.5px;font-weight:700}.control[_ngcontent-%COMP%]{margin-bottom:16px}.label-row[_ngcontent-%COMP%]{display:flex;justify-content:space-between;font-size:11px;margin-bottom:6px;opacity:.8}.val[_ngcontent-%COMP%]{color:#0fc;font-family:JetBrains Mono,monospace}input[type=range][_ngcontent-%COMP%]{width:100%;accent-color:#00ffcc;cursor:pointer}.input-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:16px}.input-col[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:4px}.input-col[_ngcontent-%COMP%]   label[_ngcontent-%COMP%]{font-size:10px;color:#666;font-weight:700}.input-grid[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{width:100%;background:#2a2a2a;border:1px solid #444;color:#fff;padding:10px 4px;border-radius:8px;text-align:center;font-size:14px}.action-buttons[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:10px}.add-btn[_ngcontent-%COMP%]{background:#0fc;color:#000;padding:12px;border:none;border-radius:8px;cursor:pointer;font-weight:800;font-size:11px;text-transform:uppercase}.undo-btn[_ngcontent-%COMP%]{background:transparent;color:#f44;padding:10px;border:1px solid rgba(255,68,68,.3);border-radius:8px;cursor:pointer;font-size:11px}.reset-btn[_ngcontent-%COMP%]{width:100%;padding:10px;background:#222;color:#888;border:none;border-radius:8px;cursor:pointer;margin-top:10px;font-size:11px}.hint[_ngcontent-%COMP%]{font-size:9px;color:#555;margin-top:15px;text-align:center;line-height:1.4}"],changeDetection:0})};_v(hp,qE).catch(n=>console.error(n));
