using P2PDataShare.Signaling.Hubs;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.WithOrigins("http://localhost:5173").AllowAnyHeader().AllowAnyMethod().AllowCredentials();
    });
});

var app = builder.Build();
app.UseCors();
app.MapHub<SignalingHub>("/hubs/signaling");
app.MapGet("/health", () => Results.Ok(new { status = "Signaling Server Running" }));
app.Run();