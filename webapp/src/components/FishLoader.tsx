import { useEffect, useRef } from 'react';

interface FishLoaderProps {
    /** Rendered CSS width in px. Height is kept at a 4:3 ratio. */
    size?: number;
    className?: string;
    /** Accessible label. When omitted the canvas is treated as decorative. */
    label?: string;
}

interface Bubble {
    x: number;
    y: number;
    vx: number;
    vy: number;
    life: number;
    size: number;
}

/**
 * Animated fish swimming in a circular orbit, drawn on a canvas.
 * Used as the app's loading indicator. The canvas draws at a fixed 400×300
 * internal resolution and is scaled down via CSS with the `size` prop.
 */
export default function FishLoader({ size = 200, className, label }: FishLoaderProps) {
    const canvasRef = useRef<HTMLCanvasElement>(null);

    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        if (!ctx) return;

        let animationFrameId = 0;
        let time = 0;
        const particles: Bubble[] = [];

        const cfg = {
            fishColor: '#2f6f9f', // Steel Blue — darker for contrast on light backgrounds
            pathColor: '#e8e8e8', // Light Grey — subtle orbit path
            fishLength: 100,
            fishWidth: 26,
            tailSize: 1.2,
            pectoralSize: 1.0,
            dorsalSize: 1.0,
            speed: 2,
            wiggle: 6,
            bubbles: 35,
            bubbleSpawnDist: 10,
            bubbleLife: 1.0,
        };

        const getAngle = (p1: { x: number; y: number }, p2: { x: number; y: number }) =>
            Math.atan2(p2.y - p1.y, p2.x - p1.x);

        const render = () => {
            const width = canvas.width;
            const height = canvas.height;
            const cx = width / 2;
            const cy = height / 2;
            const orbitRadius = 70;

            ctx.clearRect(0, 0, width, height);

            // Draw Orbit Path
            ctx.beginPath();
            ctx.arc(cx, cy, orbitRadius, 0, Math.PI * 2);
            ctx.strokeStyle = cfg.pathColor;
            ctx.lineWidth = 2;
            ctx.stroke();

            time += cfg.speed * 0.02;
            const numSegments = 30;
            const spine: { x: number; y: number }[] = [];

            for (let i = 0; i < numSegments; i++) {
                const distFromHead = i * (cfg.fishLength / numSegments);
                const baseAngle = time - (distFromHead / orbitRadius);
                const bx = cx + orbitRadius * Math.cos(baseAngle);
                const by = cy + orbitRadius * Math.sin(baseAngle);
                const nx = Math.cos(baseAngle);
                const ny = Math.sin(baseAngle);
                const envelope = (i / numSegments);
                const wave = Math.sin(time * 5 - i * 0.3) * cfg.wiggle * envelope;
                spine.push({ x: bx + nx * wave, y: by + ny * wave });
            }

            // Draw Bubbles
            if (Math.random() < cfg.bubbles / 100) {
                const tail = spine[numSegments - 1];
                const prev = spine[numSegments - 5];
                const angle = Math.atan2(tail.y - prev.y, tail.x - prev.x);
                particles.push({
                    x: tail.x - Math.cos(angle) * cfg.bubbleSpawnDist,
                    y: tail.y - Math.sin(angle) * cfg.bubbleSpawnDist,
                    vx: (Math.random() - 0.5) * 0.5,
                    vy: (Math.random() - 0.5) * 0.5,
                    life: cfg.bubbleLife,
                    size: Math.random() * 5 + 2,
                });
            }

            for (let i = particles.length - 1; i >= 0; i--) {
                const p = particles[i];
                p.life -= 0.008;
                if (p.life <= 0) {
                    particles.splice(i, 1);
                } else {
                    p.x += p.vx;
                    p.y += p.vy;
                    ctx.beginPath();
                    ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
                    ctx.strokeStyle = `rgba(47, 111, 159, ${p.life})`;
                    ctx.lineWidth = 1.2;
                    ctx.stroke();
                }
            }

            ctx.fillStyle = cfg.fishColor;

            // Dorsal
            const dStart = spine[9];
            const dMid = spine[13];
            const dEnd = spine[17];
            const dAngle = getAngle(spine[9], spine[17]);
            ctx.beginPath();
            ctx.moveTo(dStart.x, dStart.y);
            ctx.lineTo(dMid.x + Math.cos(dAngle - 1.2) * 18 * cfg.dorsalSize, dMid.y + Math.sin(dAngle - 1.2) * 18 * cfg.dorsalSize);
            ctx.lineTo(dEnd.x, dEnd.y);
            ctx.fill();

            // Pectoral
            const pMid = spine[9];
            const pAngle = getAngle(spine[6], spine[13]);
            ctx.beginPath();
            ctx.moveTo(spine[6].x, spine[6].y);
            ctx.lineTo(pMid.x + Math.cos(pAngle + 1.2) * 24 * cfg.pectoralSize, pMid.y + Math.sin(pAngle + 1.2) * 24 * cfg.pectoralSize);
            ctx.lineTo(spine[13].x, spine[13].y);
            ctx.fill();

            // Body
            ctx.beginPath();
            for (let i = 0; i < numSegments; i++) {
                const p = spine[i];
                const angle = getAngle(spine[Math.max(i - 1, 0)], spine[Math.min(i + 1, numSegments - 1)]);
                const w = (cfg.fishWidth / 2) * Math.sin(Math.pow(i / (numSegments - 1), 0.6) * Math.PI);
                ctx.lineTo(p.x + Math.cos(angle + Math.PI / 2) * w, p.y + Math.sin(angle + Math.PI / 2) * w);
            }
            for (let i = numSegments - 1; i >= 0; i--) {
                const p = spine[i];
                const angle = getAngle(spine[Math.max(i - 1, 0)], spine[Math.min(i + 1, numSegments - 1)]);
                const w = (cfg.fishWidth / 2) * Math.sin(Math.pow(i / (numSegments - 1), 0.6) * Math.PI);
                ctx.lineTo(p.x + Math.cos(angle - Math.PI / 2) * w, p.y + Math.sin(angle - Math.PI / 2) * w);
            }
            ctx.closePath();
            ctx.fill();

            // Rounded Tail
            const t = spine[numSegments - 1];
            const a = getAngle(spine[numSegments - 5], t);
            const len = 30 * cfg.tailSize;
            ctx.beginPath();
            ctx.moveTo(t.x, t.y);
            ctx.quadraticCurveTo(t.x + Math.cos(a - 0.6) * len, t.y + Math.sin(a - 0.6) * len, t.x + Math.cos(a) * len * 0.4, t.y + Math.sin(a) * len * 0.4);
            ctx.quadraticCurveTo(t.x + Math.cos(a + 0.6) * len, t.y + Math.sin(a + 0.6) * len, t.x, t.y);
            ctx.fill();

            animationFrameId = requestAnimationFrame(render);
        };

        render();
        return () => cancelAnimationFrame(animationFrameId);
    }, []);

    const height = Math.round(size * 0.75);

    return (
        <canvas
            ref={canvasRef}
            width={400}
            height={300}
            className={className}
            style={{ width: size, height }}
            role={label ? 'img' : undefined}
            aria-label={label}
            aria-hidden={label ? undefined : true}
        />
    );
}
