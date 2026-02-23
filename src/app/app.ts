import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, NgZone, ChangeDetectorRef, ChangeDetectionStrategy } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

interface VectorInfo {
  polar: number;
  azimuth: number;
  label: string;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [FormsModule, CommonModule],
  templateUrl: './app.html',
  styleUrl: './app.css',
  changeDetection: ChangeDetectionStrategy.OnPush 
})
export class AppComponent implements AfterViewInit, OnDestroy {
  @ViewChild('rendererContainer') rendererContainer!: ElementRef;

  // Соответствие: A -> Z (Синяя), B -> X (Красная), C -> Y (Зеленая)
  public rotation: { [key: string]: number } = { A: 0, B: 0, C: 0 };
  public newDir = { x: '', y: '', z: '' };
  public lastCalculated: VectorInfo | null = null;

  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private controls!: OrbitControls;
  private frameId: number | null = null;
  private mainGroup!: THREE.Group;
  private customGroup!: THREE.Group;

  constructor(private ngZone: NgZone, private cdr: ChangeDetectorRef) {}

  public resetRotation() {
    this.rotation = { A: 0, B: 0, C: 0 };
    if (this.mainGroup) this.mainGroup.rotation.set(0, 0, 0);
    this.controls.reset();
    this.lastCalculated = null;
    this.cdr.detectChanges();
  }

  public undoLastDirection() {
    if (this.customGroup.children.length > 0) {
      for (let i = 0; i < 3; i++) {
        const lastChild = this.customGroup.children[this.customGroup.children.length - 1];
        if (lastChild) {
          if ((lastChild as any).geometry) (lastChild as any).geometry.dispose();
          if ((lastChild as any).material) (lastChild as any).material.dispose();
          this.customGroup.remove(lastChild);
        }
      }
      this.lastCalculated = null;
      this.cdr.detectChanges();
    }
  }

  public addCustomDirection() {
    const x = parseFloat(this.newDir.x || '0');
    const y = parseFloat(this.newDir.y || '0');
    const z = parseFloat(this.newDir.z || '0');

    if (x === 0 && y === 0 && z === 0) return;

    const vec = new THREE.Vector3(x, y, z);
    const r = vec.length();
    
    // Сферические координаты (углы "протыкания" сферы)
    const polar = Math.acos(y / r) * (180 / Math.PI);
    const azimuth = Math.atan2(z, x) * (180 / Math.PI);

    this.lastCalculated = {
      polar,
      azimuth,
      label: `[${z} ${x} ${y}]`
    };

    const color = new THREE.Color(0x00ffcc);
    const norm = vec.clone().normalize();
    
    const line = new THREE.Line(
      new THREE.BufferGeometry().setFromPoints([new THREE.Vector3(0,0,0), norm.clone().multiplyScalar(4)]),
      new THREE.LineBasicMaterial({ color, linewidth: 2 })
    );
    
    const dot = new THREE.Mesh(
      new THREE.SphereGeometry(0.08, 20, 20), 
      new THREE.MeshBasicMaterial({ color })
    );
    dot.position.copy(norm.clone().multiplyScalar(4));
    
    const canvas = this.createCanvasLabel(`${z}|${x}|${y}`, 34, 0x00ffcc, true);
    const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(canvas) }));
    sprite.position.copy(norm.clone().multiplyScalar(4.4));
    sprite.scale.set(0.8, 0.4, 1);

    this.customGroup.add(line, dot, sprite);
    this.newDir = { x: '', y: '', z: '' };
    this.cdr.detectChanges();
  }

  ngAfterViewInit() {
    this.ngZone.runOutsideAngular(() => {
      this.initThree();
      this.createSceneObjects();
      this.animate();
      
      window.addEventListener('resize', () => this.onWindowResize());
    });
  }

  private initThree() {
    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x050505);
    const container = this.rendererContainer.nativeElement;
    
    this.camera = new THREE.PerspectiveCamera(75, container.clientWidth / container.clientHeight, 0.1, 1000);
    this.camera.position.set(6, 6, 10);

    this.renderer = new THREE.WebGLRenderer({ 
      antialias: true, 
      precision: "highp" 
    });
    
    this.renderer.setPixelRatio(window.devicePixelRatio);
    this.renderer.setSize(container.clientWidth, container.clientHeight);
    
    // Настройка для насыщенных цветов
    this.renderer.outputColorSpace = THREE.SRGBColorSpace;
    
    container.appendChild(this.renderer.domElement);

    this.scene.add(new THREE.AmbientLight(0xffffff, 0.8));
    const light = new THREE.DirectionalLight(0xffffff, 1.5);
    light.position.set(5, 10, 7);
    this.scene.add(light);

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
  }

  private onWindowResize() {
    const container = this.rendererContainer.nativeElement;
    this.camera.aspect = container.clientWidth / container.clientHeight;
    this.camera.updateProjectionMatrix();
    this.renderer.setSize(container.clientWidth, container.clientHeight);
  }

  private createSceneObjects() {
    this.mainGroup = new THREE.Group();
    this.mainGroup.rotation.order = 'ZYX'; 
    this.customGroup = new THREE.Group();
    this.mainGroup.add(this.customGroup);

    // Сфера
    this.mainGroup.add(new THREE.Mesh(
      new THREE.SphereGeometry(3, 64, 64),
      new THREE.MeshStandardMaterial({ color: 0x444444, wireframe: true, transparent: true, opacity: 0.1 })
    ));

    // Оси (Helper + подписи)
    const axes = new THREE.AxesHelper(5);
    this.scene.add(axes);

    this.addAxisLabel('B', new THREE.Vector3(5.5, 0, 0), 0xff4444); 
    this.addAxisLabel('C', new THREE.Vector3(0, 5.5, 0), 0x44ff44); 
    this.addAxisLabel('A', new THREE.Vector3(0, 0, 5.5), 0x4444ff); 

    this.scene.add(this.mainGroup);
  }

  private createCanvasLabel(text: string, fontSize: number, color: number, isMono: boolean = false) {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d')!;
    canvas.width = 512; canvas.height = 256;
    ctx.font = `${isMono ? '' : 'Bold'} ${fontSize * 2}px ${isMono ? 'monospace' : 'Inter, Arial'}`;
    ctx.fillStyle = `#${new THREE.Color(color).getHexString()}`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    if (!isMono) {
      ctx.fillText(text, 256, 128);
    } else {
      const charWidth = (fontSize * 2) * 0.6;
      let currentX = 256 - (text.replace(/-/g, '').length * charWidth) / 2 + charWidth / 2;
      for (let char of text) {
        if (char === '-') {
          ctx.fillRect(currentX - charWidth/2 + 5, 128 - 2, charWidth - 10, 4);
        } else {
          ctx.fillText(char, currentX, 128);
          currentX += charWidth;
        }
      }
    }
    return canvas;
  }

  private addAxisLabel(text: string, position: THREE.Vector3, color: number) {
    const canvas = this.createCanvasLabel(text, 40, color);
    const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(canvas) }));
    sprite.position.copy(position);
    sprite.scale.set(1.2, 0.6, 1);
    this.scene.add(sprite);
  }

  private animate() {
    this.frameId = requestAnimationFrame(() => this.animate());
    if (this.mainGroup) {
      this.mainGroup.rotation.z = this.rotation['A'];
      this.mainGroup.rotation.x = this.rotation['B'];
      this.mainGroup.rotation.y = this.rotation['C'];
    }
    this.controls.update();
    this.renderer.render(this.scene, this.camera);
  }

  ngOnDestroy() {
    if (this.frameId) cancelAnimationFrame(this.frameId);
    this.renderer.dispose();
    window.removeEventListener('resize', () => this.onWindowResize());
  }
}