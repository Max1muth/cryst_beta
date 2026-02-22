import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, NgZone, ChangeDetectorRef, ChangeDetectionStrategy } from '@angular/core';
import { FormsModule } from '@angular/forms';
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [FormsModule],
  templateUrl: './app.html',
  styleUrl: './app.css',
  changeDetection: ChangeDetectionStrategy.OnPush 
})
export class AppComponent implements AfterViewInit, OnDestroy {
  @ViewChild('rendererContainer') rendererContainer!: ElementRef;

  // Модель вращения: A (Z), B (X), C (Y)
  public rotation = { A: 0, B: 0, C: 0 };

  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private controls!: OrbitControls;
  private frameId: number | null = null;
  private mainGroup!: THREE.Group;

  constructor(
    private ngZone: NgZone,
    private cdr: ChangeDetectorRef // Добавляем для ручного обновления UI
  ) {}

  public resetRotation() {
    this.rotation = { A: 0, B: 0, C: 0 };
    if (this.mainGroup) {
      this.mainGroup.rotation.set(0, 0, 0);
    }
    this.controls.reset();
    this.cdr.detectChanges();
  }

  ngAfterViewInit() {
    this.ngZone.runOutsideAngular(() => {
      this.initThree();
      this.createSceneObjects();
      this.setupControlsSync(); // Настраиваем синхронизацию
      this.animate();
    });
  }

  private initThree() {
    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x050505);

    const container = this.rendererContainer.nativeElement;
    this.camera = new THREE.PerspectiveCamera(75, container.clientWidth / container.clientHeight, 0.1, 1000);
    this.camera.position.set(5, 5, 8);

    this.renderer = new THREE.WebGLRenderer({ antialias: true, precision: "lowp" });
    this.renderer.setSize(container.clientWidth, container.clientHeight);
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    container.appendChild(this.renderer.domElement);

    this.scene.add(new THREE.AmbientLight(0xffffff, 0.4));
    const light = new THREE.PointLight(0xffffff, 120);
    light.position.set(10, 10, 10);
    this.scene.add(light);

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
  }

  // --- НОВЫЙ МЕТОД ДЛЯ СИНХРОНИЗАЦИИ ---
  private setupControlsSync() {
    this.controls.addEventListener('change', () => {
      // Если OrbitControls меняет камеру, мы можем захотеть 
      // пересчитать вращение объекта, если бы объект "следовал" за ней.
      // Но в твоем случае объект вращается сам.
      // Если ты хочешь, чтобы при вращении МЫШКОЙ самого объекта 
      // менялись ползунки, нужно убедиться, что OrbitControls вращает ГРУППУ.
      // По умолчанию OrbitControls вращает КАМЕРУ вокруг центра.
    });
  }

  private createSceneObjects() {
    this.mainGroup = new THREE.Group();

    const sphereGeom = new THREE.SphereGeometry(3, 32, 32);
    const sphereMat = new THREE.MeshStandardMaterial({ 
      color: 0x888888, wireframe: true, transparent: true, opacity: 0.03 
    });
    this.mainGroup.add(new THREE.Mesh(sphereGeom, sphereMat));

    const octGeom = new THREE.OctahedronGeometry(1.5, 0);
    const octMat = new THREE.MeshStandardMaterial({ color: 0xffffff, wireframe: true, transparent: true, opacity: 0.15 });
    this.mainGroup.add(new THREE.Mesh(octGeom, octMat));

    this.addAxisLabel('B', new THREE.Vector3(5.2, 0, 0), 0xff4444); 
    this.addAxisLabel('C', new THREE.Vector3(0, 5.2, 0), 0x44ff44); 
    this.addAxisLabel('A', new THREE.Vector3(0, 0, 5.2), 0x4444ff); 

    const directions = [
      { vec: new THREE.Vector3( 1,  1,  1), label: '1|1|1' },
      { vec: new THREE.Vector3( 1,  1, -1), label: '-1|1|1' },
      { vec: new THREE.Vector3(-1,  1, -1), label: '-1|-1|1' },
      { vec: new THREE.Vector3(-1,  1,  1), label: '1|-1|1' },
      { vec: new THREE.Vector3( 1, -1,  1), label: '1|1|-1' },
      { vec: new THREE.Vector3( 1, -1, -1), label: '-1|1|-1' },
      { vec: new THREE.Vector3(-1, -1, -1), label: '-1|-1|-1' },
      { vec: new THREE.Vector3(-1, -1,  1), label: '1|-1|-1' },
    ];

    const baseHue = 0.55; 
    directions.forEach((d, index) => {
      const normalizedDir = d.vec.clone().normalize();
      const lightness = 0.2 + (index * 0.09);
      const color = new THREE.Color().setHSL(baseHue, 0.7, lightness);

      const linePoints = [new THREE.Vector3(0, 0, 0), normalizedDir.clone().multiplyScalar(4)];
      const lineGeom = new THREE.BufferGeometry().setFromPoints(linePoints);
      const line = new THREE.Line(lineGeom, new THREE.LineBasicMaterial({ color: color }));
      
      const dot = new THREE.Mesh(new THREE.SphereGeometry(0.06, 8, 8), new THREE.MeshBasicMaterial({ color: color }));
      dot.position.copy(linePoints[1]);
      
      this.addDirectionLabel(d.label, normalizedDir.clone().multiplyScalar(4.3), color);
      this.mainGroup.add(line, dot);
    });

    this.scene.add(this.mainGroup);
    this.scene.add(new THREE.AxesHelper(5));
  }

  // --- ЛОГИКА ОТРИСОВКИ МИНУСОВ СВЕРХУ (Исправленная) ---
  private createCanvasLabel(text: string, fontSize: number, color: number, isMono: boolean = false) {
    const canvas = document.createElement('canvas');
    const context = canvas.getContext('2d')!;
    canvas.width = 256; canvas.height = 128;
    
    context.font = `${isMono ? '' : 'Bold'} ${fontSize}px ${isMono ? 'monospace' : 'Arial'}`;
    context.fillStyle = `#${new THREE.Color(color).getHexString()}`;
    context.textAlign = 'center';
    context.textBaseline = 'middle';

    if (!isMono) {
      context.fillText(text, 128, 64);
    } else {
      const charWidth = fontSize * 0.6;
      const cleanText = text.replace(/-/g, '');
      let currentX = 128 - (cleanText.length * charWidth) / 2 + charWidth / 2;

      for (let i = 0; i < text.length; i++) {
        const char = text[i];
        if (char === '-') {
          context.fillRect(currentX - charWidth/2 + 2, 64 - fontSize/2, charWidth - 4, fontSize/10);
        } else {
          context.fillText(char, currentX, 64);
          currentX += charWidth;
        }
      }
    }
    return canvas;
  }

  private addAxisLabel(text: string, position: THREE.Vector3, color: number) {
    const canvas = this.createCanvasLabel(text, 80, color);
    const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(canvas) }));
    sprite.position.copy(position);
    sprite.scale.set(0.6, 0.6, 1);
    this.scene.add(sprite);
  }

  private addDirectionLabel(text: string, position: THREE.Vector3, color: THREE.Color) {
    const canvas = this.createCanvasLabel(text, 34, color.getHex(), true);
    const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(canvas) }));
    sprite.position.copy(position);
    sprite.scale.set(0.7, 0.35, 1);
    this.mainGroup.add(sprite);
  }

  // ОБНОВЛЕННЫЙ ЦИКЛ АНИМАЦИИ
  private animate() {
    this.frameId = requestAnimationFrame(() => this.animate());
    
    if (this.mainGroup) {
      // 1. Применяем вращение ИЗ ползунков к объекту
      this.mainGroup.rotation.z = this.rotation.A;
      this.mainGroup.rotation.x = this.rotation.B;
      this.mainGroup.rotation.y = this.rotation.C;
    }

    this.controls.update();
    this.renderer.render(this.scene, this.camera);
  }

  ngOnDestroy() {
    this.ngZone.runOutsideAngular(() => {
      if (this.frameId) cancelAnimationFrame(this.frameId);
      this.renderer.dispose();
      this.scene.clear();
    });
  }
}