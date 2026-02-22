import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, NgZone, ChangeDetectorRef, ChangeDetectionStrategy } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

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

  // Порядок в UI и вращение: A (Z), B (X), C (Y)
  public rotation: { [key: string]: number } = { A: 0, B: 0, C: 0 };
  public newDir = { a: '', b: '', c: '' };

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
      this.cdr.detectChanges();
    }
  }

  public addCustomDirection() {
    const valA = parseFloat(this.newDir.a || '0'); // На нас (Z)
    const valB = parseFloat(this.newDir.b || '0'); // Вправо (X)
    const valC = parseFloat(this.newDir.c || '0'); // Вверх (Y)

    if (valA === 0 && valB === 0 && valC === 0) return;

    // ВАЖНО: Мапинг в правую тройку Three.js
    // A -> Z, B -> X, C -> Y
    const vec = new THREE.Vector3(valB, valC, valA);
    
    const label = `${valA}|${valB}|${valC}`;
    const color = new THREE.Color().setHSL(Math.random(), 0.8, 0.6);
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
    
    const canvas = this.createCanvasLabel(label, 36, color.getHex(), true);
    const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(canvas) }));
    sprite.position.copy(norm.clone().multiplyScalar(4.4));
    sprite.scale.set(0.9, 0.45, 1);

    this.customGroup.add(line, dot, sprite);
    this.newDir = { a: '', b: '', c: '' };
    this.cdr.detectChanges();
  }

  ngAfterViewInit() {
    this.ngZone.runOutsideAngular(() => {
      this.initThree();
      this.createSceneObjects();
      this.animate();
    });
  }

  private initThree() {
    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x050505);
    const container = this.rendererContainer.nativeElement;
    
    this.camera = new THREE.PerspectiveCamera(75, container.clientWidth / container.clientHeight, 0.1, 1000);
    this.camera.position.set(8, 6, 8); // Удобный ракурс на правую тройку

    this.renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    this.renderer.setPixelRatio(window.devicePixelRatio);
    this.renderer.setSize(container.clientWidth, container.clientHeight);
    container.appendChild(this.renderer.domElement);

    this.scene.add(new THREE.AmbientLight(0xffffff, 1));
    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
  }

  private createSceneObjects() {
    this.mainGroup = new THREE.Group();
    this.mainGroup.rotation.order = 'ZYX'; // Порядок для кристаллографии
    this.customGroup = new THREE.Group();
    this.mainGroup.add(this.customGroup);

    this.mainGroup.add(new THREE.Mesh(
      new THREE.SphereGeometry(3, 64, 64),
      new THREE.MeshStandardMaterial({ color: 0xffffff, wireframe: true, transparent: true, opacity: 0.05 })
    ));

    const axes = new THREE.AxesHelper(5);
    this.scene.add(axes);

    // Подписи осей в правой тройке
    this.addAxisLabel('A', new THREE.Vector3(0, 0, 5.5), 0x4444ff); // Z
    this.addAxisLabel('B', new THREE.Vector3(5.5, 0, 0), 0xff4444); // X
    this.addAxisLabel('C', new THREE.Vector3(0, 5.5, 0), 0x44ff44); // Y

    this.scene.add(this.mainGroup);
  }

  private createCanvasLabel(text: string, fontSize: number, color: number, isMono: boolean = false) {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d')!;
    canvas.width = 512; canvas.height = 256;
    ctx.font = `Bold ${fontSize * 2}px monospace`;
    ctx.fillStyle = `#${new THREE.Color(color).getHexString()}`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    const charWidth = (fontSize * 2) * 0.7;
    const cleanText = text.replace(/-/g, '');
    let currentX = 256 - (cleanText.length * charWidth) / 2 + charWidth / 2;

    for (let i = 0; i < text.length; i++) {
      const char = text[i];
      if (char === '-') {
        ctx.fillRect(currentX - charWidth/2 + 5, 128 - (fontSize * 1.1), charWidth - 10, 8);
      } else {
        ctx.fillText(char, currentX, 128);
        currentX += charWidth;
      }
    }
    return canvas;
  }

  private addAxisLabel(text: string, position: THREE.Vector3, color: number) {
    const canvas = this.createCanvasLabel(text, 60, color);
    const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(canvas) }));
    sprite.position.copy(position);
    sprite.scale.set(0.8, 0.4, 1);
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
  }
}