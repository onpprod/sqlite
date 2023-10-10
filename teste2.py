import asyncio

class MinhaClasse:
    def __init__(self):
        self.valor = 0

    async def metodo_assincrono(self):
        print("Início do método assíncrono")
        await asyncio.sleep(1)  # Simula uma operação assíncrona
        self.valor += 1
        print("Fim do método assíncrono")

async def main():
    minha_instancia = MinhaClasse()
    await minha_instancia.metodo_assincrono()

if __name__ == "__main__":
    asyncio.run(main())
