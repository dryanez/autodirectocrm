-- ═══════════════════════════════════════════════════════════
-- WhatsApp FAQ / Knowledge Base table
-- Run this in Supabase SQL Editor
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS wa_faq (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    category TEXT DEFAULT 'general',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- RLS
ALTER TABLE wa_faq ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service full access on wa_faq"
    ON wa_faq
    USING (true)
    WITH CHECK (true);

-- Auto-update trigger
CREATE OR REPLACE FUNCTION update_wa_faq_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS wa_faq_updated_at ON wa_faq;
CREATE TRIGGER wa_faq_updated_at
    BEFORE UPDATE ON wa_faq
    FOR EACH ROW
    EXECUTE FUNCTION update_wa_faq_updated_at();

-- Seed some starter FAQ entries
INSERT INTO wa_faq (question, answer, category) VALUES
    ('¿Cuánto cobran de comisión?', 'La comisión por consignación es del 3.9% del precio final de venta, con un mínimo de $150.000 CLP.', 'consignacion'),
    ('¿Dónde están ubicados?', 'Estamos en Avenida Bosques de Montemar 30, Oficina 316, Viña del Mar. Horario: Lunes a Viernes 9am-7pm, Sábado 10am-2pm.', 'horario'),
    ('¿Tienen financiamiento?', 'Sí, trabajamos con bancos como Scotiabank con tasas desde 0.89% mensual. Pie desde 20% y plazo hasta 60 meses.', 'financiamiento'),
    ('¿Los autos tienen garantía?', 'Todos nuestros vehículos incluyen garantía mecánica de 3 meses en motor y transmisión.', 'garantia'),
    ('¿Cómo es el proceso de consignación?', 'Entregas tu vehículo, nosotros nos encargamos de las fotos, publicación, negociación y trámites. Una vez vendido, te transferimos el dinero menos la comisión.', 'consignacion'),
    ('¿Hacen compra directa?', 'Sí, podemos comprar tu auto directamente a precio de mercado. El proceso es rápido y el pago es el mismo día.', 'ventas'),
    ('¿Qué trámites incluyen?', 'Nos encargamos de todo: transferencia (cambio de dominio), SOAP, y documentación. El comprador no tiene que preocuparse de nada.', 'tramites')
ON CONFLICT DO NOTHING;
