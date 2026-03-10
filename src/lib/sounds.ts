let audioCtx: AudioContext | null = null;
let soundEnabled = true;

// Read preference from localStorage
try {
  const stored = localStorage.getItem("nightshift-sound");
  if (stored === "false") soundEnabled = false;
} catch {}

async function getCtx(): Promise<AudioContext> {
  if (!audioCtx) audioCtx = new AudioContext();
  if (audioCtx.state === "suspended") await audioCtx.resume();
  return audioCtx;
}

export function isSoundEnabled(): boolean {
  return soundEnabled;
}

export function setSoundEnabled(enabled: boolean) {
  soundEnabled = enabled;
  try {
    localStorage.setItem("nightshift-sound", String(enabled));
  } catch {}
}

export async function playSound(type: "success" | "error" | "complete") {
  if (!soundEnabled) return;
  try {
    const ctx = await getCtx();
    const gain = ctx.createGain();
    gain.connect(ctx.destination);
    gain.gain.value = 0.15;

    if (type === "success") {
      const osc = ctx.createOscillator();
      osc.type = "sine";
      osc.frequency.value = 880;
      osc.connect(gain);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.1);
      osc.onended = () => { osc.disconnect(); gain.disconnect(); };
    } else if (type === "error") {
      const osc = ctx.createOscillator();
      osc.type = "sine";
      osc.frequency.value = 220;
      osc.connect(gain);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.2);
      osc.onended = () => { osc.disconnect(); gain.disconnect(); };
    } else if (type === "complete") {
      gain.disconnect(); // Not used for arpeggio — disconnect the outer gain
      const notes = [523, 659, 784];
      notes.forEach((freq, i) => {
        const osc = ctx.createOscillator();
        osc.type = "sine";
        osc.frequency.value = freq;
        const g = ctx.createGain();
        g.gain.value = 0.12;
        g.connect(ctx.destination);
        osc.connect(g);
        osc.start(ctx.currentTime + i * 0.12);
        osc.stop(ctx.currentTime + i * 0.12 + 0.15);
        osc.onended = () => { osc.disconnect(); g.disconnect(); };
      });
    }
  } catch {
    // Audio not available
  }
}
